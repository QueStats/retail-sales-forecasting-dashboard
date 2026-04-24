library(DBI)
library(RSQLite)
library(readr)
library(lubridate)

# ============================================================
# Retail Sales Dashboard Pipeline
# Purpose:
# Clean retail sales data, build SQL summary tables, create
# forecasting features without leakage, fit a simple regression
# model, and export Tableau-ready CSV files.
# ============================================================

# ----------------------------
# 1. Paths
# ----------------------------

input_file <- "retail-sales-forecasting-dashboard/data/superstore_final_dataset.csv"
db_file <- "superstore.db"
sql_file <- "retail-sales-forecasting-dashboard/sql/analysis_queries.sql"
results_dir <- "retail-sales-forecasting-dashboard/results"

if (!dir.exists(results_dir)) {
  dir.create(results_dir, recursive = TRUE)
}

# ----------------------------
# 2. Load and clean data
# ----------------------------

df <- read_csv(input_file, show_col_types = FALSE)

df$Order_Date <- parse_date_time(df$Order_Date, orders = c("mdy", "dmy"))
df$Order_Date <- as.Date(df$Order_Date)
df$Ship_Date <- as.Date(df$Ship_Date, format = "%d/%m/%Y")

df$Order_Date <- as.character(df$Order_Date)
df$Ship_Date <- as.character(df$Ship_Date)

# ----------------------------
# 3. Connect to SQLite and load cleaned data
# ----------------------------

con <- dbConnect(SQLite(), db_file)
dbWriteTable(con, "superstore", df, overwrite = TRUE)

# ----------------------------
# 4. Run SQL summary-table script
# ----------------------------

run_sql_script <- function(con, file_path) {
  sql_text <- paste(readLines(file_path, warn = FALSE), collapse = "\n")
  sql_text <- gsub("--.*", "", sql_text)
  statements <- unlist(strsplit(sql_text, ";", fixed = TRUE))
  statements <- trimws(statements)
  statements <- statements[nchar(statements) > 0]
  
  for (statement in statements) {
    cat("\nRUNNING:\n", statement, "\n")
    dbExecute(con, statement)
  }
}

run_sql_script(con, sql_file)

# Check tables were created
print(dbListTables(con))

# ----------------------------
# 5. Pull SQL outputs into R + save to results/
# ----------------------------

sql_tables <- c(
  "monthly_sales",
  "sales_features_base",
  "category_sales",
  "region_sales",
  "top_customers",
  "shipping_summary",
  "subcategory_sales",
  "region_month_sales",
  "subcategory_month_sales"
)

for (tbl in sql_tables) {
  data <- dbGetQuery(con, paste0("SELECT * FROM ", tbl))
  write.csv(
    data,
    file.path(results_dir, paste0(tbl, ".csv")),
    row.names = FALSE
  )
  
  assign(tbl, data)
}

# Clean feature table for modeling
sales_features <- na.omit(sales_features_base)

sales_features$growth_pct <- round(
  (sales_features$revenue - sales_features$prev_month) / sales_features$prev_month,
  4
)

sales_features$residual_from_roll <- round(
  sales_features$revenue - sales_features$rolling_avg_3,
  2
)

# ----------------------------
# 6. Modeling
# ----------------------------

model_data <- sales_features

n <- nrow(model_data)
train_n <- floor(0.8 * n)

train <- model_data[1:train_n, ]
test <- model_data[(train_n + 1):n, ]

# Baseline model: previous month.
pred_naive <- test$prev_month

# Regression model using only lagged information.
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
# 7. Region decision table
# ----------------------------

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
# 8. Subcategory decision table
# ----------------------------

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
# 9. Month strategy table
# ----------------------------

month_profile <- aggregate(
  cbind(revenue, growth_pct, residual_from_roll) ~ month_num,
  data = sales_features,
  FUN = function(x) c(
    avg = mean(x, na.rm = TRUE),
    sd = sd(x, na.rm = TRUE)
  )
)

month_profile <- data.frame(
  month_num = month_profile$month_num,
  avg_revenue = month_profile$revenue[, "avg"],
  revenue_sd = month_profile$revenue[, "sd"],
  avg_growth = month_profile$growth_pct[, "avg"],
  growth_sd = month_profile$growth_pct[, "sd"],
  avg_residual = month_profile$residual_from_roll[, "avg"],
  residual_sd = month_profile$residual_from_roll[, "sd"]
)

month_profile$decision <- ifelse(
  month_profile$avg_residual > 0 & month_profile$avg_growth > 0,
  "Push",
  ifelse(
    month_profile$avg_residual < 0 & month_profile$avg_growth < 0,
    "Investigate",
    "Monitor"
  )
)

month_profile <- month_profile[order(month_profile$month_num), ]

# ----------------------------
# 10. Next 3-month forecast
# ----------------------------

last_row <- tail(sales_features, 1)

future_forecast <- data.frame(
  step = 1:3,
  month_num = ((last_row$month_num + 0:2 - 1) %% 12) + 1,
  prev_month = NA_real_,
  two_months_ago = NA_real_,
  rolling_avg_3 = NA_real_
)

future_forecast$prev_month[1] <- last_row$revenue
future_forecast$two_months_ago[1] <- last_row$prev_month
future_forecast$rolling_avg_3[1] <- mean(c(
  last_row$prev_month,
  last_row$two_months_ago,
  sales_features$revenue[nrow(sales_features) - 2]
), na.rm = TRUE)

for (i in 1:3) {
  future_forecast$predicted_revenue[i] <- round(
    predict(sales_model, newdata = future_forecast[i, ]),
    2
  )

  if (i < 3) {
    future_forecast$prev_month[i + 1] <- future_forecast$predicted_revenue[i]

    if (i == 1) {
      future_forecast$two_months_ago[i + 1] <- last_row$revenue
    } else {
      future_forecast$two_months_ago[i + 1] <- future_forecast$predicted_revenue[i - 1]
    }

    prior_vals <- c(
      future_forecast$prev_month[i + 1],
      future_forecast$two_months_ago[i + 1],
      if (i == 1) last_row$prev_month else future_forecast$two_months_ago[i]
    )

    future_forecast$rolling_avg_3[i + 1] <- round(mean(prior_vals, na.rm = TRUE), 2)
  }
}

# ----------------------------
# 11. Forecast evaluation table
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
# 12. Anomaly table
# ----------------------------

anomaly_months <- sales_features[
  sales_features$anomaly_flag == "Anomaly",
  c(
    "month", "revenue", "predicted_revenue", "model_error", "abs_error",
    "growth_pct", "residual_from_roll"
  )
]

anomaly_months <- anomaly_months[order(-anomaly_months$abs_error), ]

# ----------------------------
# 13. Export CSVs for Tableau / GitHub
# ----------------------------

write.csv(monthly_sales, file.path(results_dir, "monthly_sales.csv"), row.names = FALSE)
write.csv(sales_features, file.path(results_dir, "sales_features.csv"), row.names = FALSE)
write.csv(category_sales, file.path(results_dir, "category_sales.csv"), row.names = FALSE)
write.csv(region_sales, file.path(results_dir, "region_sales.csv"), row.names = FALSE)
write.csv(top_customers, file.path(results_dir, "top_customers.csv"), row.names = FALSE)
write.csv(shipping_summary, file.path(results_dir, "shipping_summary.csv"), row.names = FALSE)
write.csv(subcategory_sales, file.path(results_dir, "subcategory_sales.csv"), row.names = FALSE)
write.csv(model_results, file.path(results_dir, "model_results.csv"), row.names = FALSE)
write.csv(region_perf, file.path(results_dir, "region_decisions.csv"), row.names = FALSE)
write.csv(subcategory_perf, file.path(results_dir, "subcategory_decisions.csv"), row.names = FALSE)
write.csv(month_profile, file.path(results_dir, "month_profile.csv"), row.names = FALSE)
write.csv(future_forecast, file.path(results_dir, "future_forecast.csv"), row.names = FALSE)
write.csv(region_month_sales, file.path(results_dir, "region_month_sales.csv"), row.names = FALSE)
write.csv(subcategory_month_sales, file.path(results_dir, "subcategory_month_sales.csv"), row.names = FALSE)
write.csv(forecast_eval, file.path(results_dir, "forecast_eval.csv"), row.names = FALSE)
write.csv(anomaly_months, file.path(results_dir, "anomaly_months.csv"), row.names = FALSE)

# ----------------------------
# 14. Output summary
# ----------------------------

cat("\n============================\n")
cat("MODEL RESULTS\n")
cat("============================\n")
print(model_results)

cat("\n============================\n")
cat("REGION DECISIONS\n")
cat("============================\n")
print(region_perf)

cat("\n============================\n")
cat("TOP SUBCATEGORY DECISIONS\n")
cat("============================\n")
print(head(subcategory_perf, 10))

cat("\n============================\n")
cat("MONTH STRATEGY\n")
cat("============================\n")
print(month_profile)

cat("\n============================\n")
cat("NEXT 3-MONTH FORECAST\n")
cat("============================\n")
print(future_forecast)

# ----------------------------
# 15. Close connection
# ----------------------------

dbDisconnect(con)
