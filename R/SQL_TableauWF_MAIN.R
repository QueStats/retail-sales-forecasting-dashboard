library(DBI)
library(RSQLite)
library(readr)
library(lubridate)

# ============================================================
# Retail Sales Dashboard Pipeline
# Purpose:
# Clean retail sales data, build summary tables, create simple
# forecasting features without leakage, and export Tableau-ready
# CSV files for dashboarding and decision analysis.
# ============================================================

project_dir <- "retail-sales-forecasting-dashboard"
results_dir <- file.path(project_dir, "results")

if (!dir.exists(results_dir)) {
  dir.create(results_dir, recursive = TRUE)
}

# ----------------------------
# 1. LOAD AND CLEAN DATA
# ----------------------------

df <- read_csv("retail-sales-forecasting-dashboard/data/superstore_final_dataset.csv", show_col_types = FALSE)

df$Order_Date <- parse_date_time(df$Order_Date, orders = c("mdy", "dmy"))
df$Order_Date <- as.Date(df$Order_Date)
df$Ship_Date  <- as.Date(df$Ship_Date, format = "%d/%m/%Y")

df$Order_Date <- as.character(df$Order_Date)
df$Ship_Date  <- as.character(df$Ship_Date)

# ----------------------------
# 2. CONNECT TO SQLITE
# ----------------------------

con <- dbConnect(SQLite(), "superstore.db")

dbWriteTable(con, "superstore", df, overwrite = TRUE)

# ----------------------------
# 3. MONTHLY SALES TABLE
# ----------------------------

dbExecute(con, "DROP TABLE IF EXISTS monthly_sales")

dbExecute(con, "
  CREATE TABLE monthly_sales AS
  SELECT
    strftime('%Y-%m', Order_Date) AS month,
    ROUND(SUM(Sales), 2) AS revenue
  FROM superstore
  GROUP BY strftime('%Y-%m', Order_Date)
  ORDER BY month
")

monthly_sales <- dbGetQuery(con, "
  SELECT *
  FROM monthly_sales
  ORDER BY month
")

# ----------------------------
# 4. TIME-SERIES FEATURES
#    No leakage: only past values are used
# ----------------------------

sales_features <- dbGetQuery(con, "
  SELECT
    month,
    revenue,
    CAST(substr(month, 1, 4) AS INTEGER) AS year,
    CAST(substr(month, 6, 2) AS INTEGER) AS month_num,
    LAG(revenue, 1) OVER (ORDER BY month) AS prev_month,
    LAG(revenue, 2) OVER (ORDER BY month) AS two_months_ago,
    ROUND(AVG(revenue) OVER (
      ORDER BY month
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ), 2) AS rolling_avg_3
  FROM monthly_sales
  ORDER BY month
")

sales_features <- na.omit(sales_features)

sales_features$growth_pct <- round(
  (sales_features$revenue - sales_features$prev_month) / sales_features$prev_month,
  4
)

sales_features$residual_from_roll <- round(
  sales_features$revenue - sales_features$rolling_avg_3,
  2
)

# ----------------------------
# 5. CATEGORY PERFORMANCE
# ----------------------------

category_sales <- dbGetQuery(con, "
  SELECT
    Category,
    ROUND(SUM(Sales), 2) AS revenue
  FROM superstore
  GROUP BY Category
  ORDER BY revenue DESC
")

category_sales$share_pct <- round(
  100 * category_sales$revenue / sum(category_sales$revenue),
  2
)

# ----------------------------
# 6. REGION PERFORMANCE
# ----------------------------

region_sales <- dbGetQuery(con, "
  SELECT
    Region,
    ROUND(SUM(Sales), 2) AS revenue
  FROM superstore
  GROUP BY Region
  ORDER BY revenue DESC
")

region_sales$share_pct <- round(
  100 * region_sales$revenue / sum(region_sales$revenue),
  2
)

# ----------------------------
# 7. TOP CUSTOMERS
# ----------------------------

top_customers <- dbGetQuery(con, "
  SELECT
    Customer_Name,
    ROUND(SUM(Sales), 2) AS revenue
  FROM superstore
  GROUP BY Customer_Name
  ORDER BY revenue DESC
  LIMIT 10
")

top_customers$total_share_pct <- round(
  100 * top_customers$revenue / sum(top_customers$revenue),
  2
)

# ----------------------------
# 8. SHIPPING PERFORMANCE
# ----------------------------

shipping_summary <- dbGetQuery(con, "
  SELECT
    Ship_Mode,
    ROUND(AVG(julianday(Ship_Date) - julianday(Order_Date)), 2) AS avg_ship_days,
    COUNT(*) AS order_count
  FROM superstore
  GROUP BY Ship_Mode
  ORDER BY avg_ship_days
")

# ----------------------------
# 9. SUBCATEGORY BREAKDOWN
# ----------------------------

subcategory_sales <- dbGetQuery(con, "
  SELECT
    Sub_Category,
    ROUND(SUM(Sales), 2) AS revenue
  FROM superstore
  GROUP BY Sub_Category
  ORDER BY revenue DESC
")

subcategory_sales$share_pct <- round(
  100 * subcategory_sales$revenue / sum(subcategory_sales$revenue),
  2
)

# ----------------------------
# 10. MODELING
# ----------------------------

model_data <- sales_features

n <- nrow(model_data)
train_n <- floor(0.8 * n)

train <- model_data[1:train_n, ]
test  <- model_data[(train_n + 1):n, ]

# Baseline model: previous month
pred_naive <- test$prev_month

# Regression model using only lagged information
sales_model <- lm(
  revenue ~ prev_month + two_months_ago + rolling_avg_3 + factor(month_num),
  data = train
)

pred_lm <- predict(sales_model, newdata = test)

mae <- function(actual, pred) {
  mean(abs(actual - pred), na.rm = TRUE)
}

rmse <- function(actual, pred) {
  sqrt(mean((actual - pred)^2, na.rm = TRUE))
}

mape <- function(actual, pred) {
  mean(abs((actual - pred) / actual), na.rm = TRUE) * 100
}

model_results <- data.frame(
  model = c("Naive Lag-1", "Regression with Lag Features"),
  MAE = c(
    mae(test$revenue, pred_naive),
    mae(test$revenue, pred_lm)
  ),
  RMSE = c(
    rmse(test$revenue, pred_naive),
    rmse(test$revenue, pred_lm)
  ),
  MAPE = c(
    mape(test$revenue, pred_naive),
    mape(test$revenue, pred_lm)
  )
)

sales_features$predicted_revenue <- round(
  predict(sales_model, newdata = sales_features),
  2
)

sales_features$model_error <- round(
  sales_features$revenue - sales_features$predicted_revenue,
  2
)

sales_features$abs_error <- abs(sales_features$model_error)

err_sd <- sd(sales_features$model_error, na.rm = TRUE)

sales_features$anomaly_flag <- ifelse(
  abs(sales_features$model_error) > 2 * err_sd,
  "Anomaly",
  "Normal"
)

# ----------------------------
# 11. REGION DECISION TABLE
# ----------------------------

region_month_sales <- dbGetQuery(con, "
  SELECT
    strftime('%Y-%m', Order_Date) AS month,
    Region,
    ROUND(SUM(Sales), 2) AS revenue
  FROM superstore
  GROUP BY strftime('%Y-%m', Order_Date), Region
  ORDER BY month, Region
")

region_perf <- aggregate(
  revenue ~ Region,
  data = region_month_sales,
  FUN = function(x) c(
    total_revenue = sum(x),
    avg_monthly_revenue = mean(x),
    volatility = sd(x),
    cv = sd(x) / mean(x)
  )
)

region_perf <- data.frame(
  Region = region_perf$Region,
  total_revenue = region_perf$revenue[, "total_revenue"],
  avg_monthly_revenue = region_perf$revenue[, "avg_monthly_revenue"],
  volatility = region_perf$revenue[, "volatility"],
  cv = region_perf$revenue[, "cv"]
)

avg_region_revenue <- mean(region_perf$total_revenue, na.rm = TRUE)
avg_region_cv <- mean(region_perf$cv, na.rm = TRUE)

region_perf$decision <- ifelse(
  region_perf$total_revenue >= avg_region_revenue & region_perf$cv <= avg_region_cv,
  "Invest",
  ifelse(
    region_perf$total_revenue >= avg_region_revenue & region_perf$cv > avg_region_cv,
    "Maintain",
    "Fix"
  )
)

region_perf <- region_perf[order(-region_perf$total_revenue), ]
region_perf$revenue_share_pct <- round(
  100 * region_perf$total_revenue / sum(region_perf$total_revenue),
  2
)

# ----------------------------
# 12. SUBCATEGORY DECISION TABLE
# ----------------------------

subcategory_month_sales <- dbGetQuery(con, "
  SELECT
    strftime('%Y-%m', Order_Date) AS month,
    Sub_Category,
    ROUND(SUM(Sales), 2) AS revenue
  FROM superstore
  GROUP BY strftime('%Y-%m', Order_Date), Sub_Category
  ORDER BY month, Sub_Category
")

subcategory_perf <- aggregate(
  revenue ~ Sub_Category,
  data = subcategory_month_sales,
  FUN = function(x) c(
    total_revenue = sum(x),
    avg_monthly_revenue = mean(x),
    volatility = sd(x),
    cv = sd(x) / mean(x)
  )
)

subcategory_perf <- data.frame(
  Sub_Category = subcategory_perf$Sub_Category,
  total_revenue = subcategory_perf$revenue[, "total_revenue"],
  avg_monthly_revenue = subcategory_perf$revenue[, "avg_monthly_revenue"],
  volatility = subcategory_perf$revenue[, "volatility"],
  cv = subcategory_perf$revenue[, "cv"]
)

avg_subcat_revenue <- mean(subcategory_perf$total_revenue, na.rm = TRUE)
avg_subcat_cv <- mean(subcategory_perf$cv, na.rm = TRUE)

subcategory_perf$decision <- ifelse(
  subcategory_perf$total_revenue >= avg_subcat_revenue & subcategory_perf$cv <= avg_subcat_cv,
  "Scale",
  ifelse(
    subcategory_perf$total_revenue >= avg_subcat_revenue & subcategory_perf$cv > avg_subcat_cv,
    "Monitor",
    "Cut / Rework"
  )
)

subcategory_perf <- subcategory_perf[order(-subcategory_perf$total_revenue), ]
subcategory_perf$revenue_share_pct <- round(
  100 * subcategory_perf$total_revenue / sum(subcategory_perf$total_revenue),
  2
)

# ----------------------------
# 13. SUBCATEGORY MONTH RESIDUALS
# ----------------------------

subcategory_month_sales$avg_subcat_month <- ave(
  subcategory_month_sales$revenue,
  subcategory_month_sales$Sub_Category,
  FUN = mean
)

subcategory_month_sales$subcategory_residual <- round(
  subcategory_month_sales$revenue - subcategory_month_sales$avg_subcat_month,
  2
)

subcategory_month_sales$residual_pct <- round(
  100 * subcategory_month_sales$subcategory_residual /
    subcategory_month_sales$avg_subcat_month,
  2
)

# ----------------------------
# 14. SUBCATEGORY SUMMARY FOR SCATTER PLOT
# ----------------------------

subcategory_summary <- aggregate(
  subcategory_residual ~ Sub_Category,
  data = subcategory_month_sales,
  FUN = function(x) c(
    avg_residual = mean(x, na.rm = TRUE),
    residual_volatility = sd(x, na.rm = TRUE)
  )
)

subcategory_summary <- data.frame(
  Sub_Category = subcategory_summary$Sub_Category,
  avg_residual = subcategory_summary$subcategory_residual[, "avg_residual"],
  residual_volatility = subcategory_summary$subcategory_residual[, "residual_volatility"]
)

subcategory_summary <- merge(
  subcategory_summary,
  subcategory_perf[, c("Sub_Category", "total_revenue", "revenue_share_pct", "decision")],
  by = "Sub_Category",
  all.x = TRUE
)

subcategory_summary <- subcategory_summary[order(-subcategory_summary$total_revenue), ]

# ----------------------------
# 15. FORECAST EVALUATION TABLE
# ----------------------------

forecast_eval <- data.frame(
  month = test$month,
  actual_revenue = test$revenue,
  naive_pred = round(pred_naive, 2),
  regression_pred = round(pred_lm, 2),
  naive_error = round(test$revenue - pred_naive, 2),
  regression_error = round(test$revenue - pred_lm, 2)
)

# ----------------------------
# 16. ANOMALY TABLE
# ----------------------------

anomaly_months <- sales_features[sales_features$anomaly_flag == "Anomaly",
                                 c("month", "revenue", "predicted_revenue",
                                   "model_error", "abs_error", "growth_pct",
                                   "residual_from_roll")]

anomaly_months <- anomaly_months[order(-anomaly_months$abs_error), ]

# ----------------------------
# 17. EXPORT CSVs FOR TABLEAU / GITHUB
# ----------------------------

write.csv(
  sales_features,
  file.path(results_dir, "monthly_model_features.csv"),
  row.names = FALSE
)

write.csv(
  subcategory_month_sales,
  file.path(results_dir, "product_month_analysis.csv"),
  row.names = FALSE
)

write.csv(
  subcategory_summary,
  file.path(results_dir, "product_summary.csv"),
  row.names = FALSE
)

write.csv(
  region_perf,
  file.path(results_dir, "region_analysis.csv"),
  row.names = FALSE
)

write.csv(
  model_results,
  file.path(results_dir, "model_results.csv"),
  row.names = FALSE
)

write.csv(
  future_forecast,
  file.path(results_dir, "future_forecast.csv"),
  row.names = FALSE
)

# ----------------------------
# 18. OUTPUT SUMMARY
# ----------------------------

cat("\n============================\n")
cat("MODEL PERFORMANCE\n")
cat("============================\n")
print(model_results)

cat("\n============================\n")
cat("REGION ANALYSIS\n")
cat("============================\n")
print(region_perf)

cat("\n============================\n")
cat("TOP PRODUCT PERFORMANCE\n")
cat("============================\n")
print(head(subcategory_summary, 10))

cat("\n============================\n")
cat("FORECAST (NEXT 3 MONTHS)\n")
cat("============================\n")
print(future_forecast)

# ----------------------------
# 19. CLOSE CONNECTION
# ----------------------------

dbDisconnect(con)