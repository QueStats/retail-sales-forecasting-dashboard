# retail-sales-forecasting-dashboard

## Retail Sales Forecasting & Business Intelligence Dashboard

## Overview  
Analyzed ~9,800 retail sales records to evaluate performance across regions, products, and time. Built a structured pipeline using SQL for data aggregation, R for feature engineering and forecasting, and Tableau for dashboarding. The project focuses on translating data into actionable business decisions.

## Tools  
SQL, R, Tableau

## Methodology  
- Used SQL to generate aggregated tables (monthly revenue, category, region, and subcategory performance)  
- Engineered time-series features in R (lag variables, rolling averages, growth rates)  
- Built a regression model using lagged revenue features to forecast sales  
- Evaluated model performance using MAE, RMSE, and MAPE  
- Developed Tableau dashboards to visualize trends, anomalies, and decision outputs  

## Key Insights  
- West region generated ~31% of total revenue and showed strong, stable performance  
- South region contributed ~17% and underperformed relative to other regions  
- Phones (~$328K) and Chairs (~$323K) were the top revenue-driving subcategories  
- Fasteners (~$3K) and Labels showed consistently low contribution and weak growth  
- Revenue exhibits clear seasonality, with peaks in September and November  

## Model Performance  
- Regression model using lagged features outperformed a naive baseline (previous-month forecast)  
- Reduced forecasting error and captured short-term revenue trends  
- Enabled identification of high-error months for anomaly detection  

## Business Decisions  
- Invest in high-performing, low-variance regions (West)  
- Maintain but stabilize high-revenue, high-volatility regions  
- Improve or restructure underperforming regions (South, Central)  
- Scale top-performing subcategories  
- Cut or rework consistently low-performing products  
- Align strategy with seasonal peaks (Q3–Q4 focus)  

## Repository Structure  
- `/data` — cleaned datasets used for analysis  
- `/sql` — aggregation and KPI queries  
- `/R` — feature engineering, modeling, and forecasting pipeline  
- `/tableau` — dashboard workbook and notes  
- `/images` — dashboard screenshots  
- `/results` — model outputs and decision tables  