# retail-sales-forecasting-dashboard

## Retail Sales Business Intelligence Dashboard

## Overview
Analyzed ~9,800 retail sales records to evaluate performance across regions, products, and time. The core deliverable is a Tableau dashboard built on SQL-aggregated data. R was used to generate supplementary model outputs and forecast CSVs that feed into the visualizations, but the modeling component is exploratory rather than production-grade.

## Tools
SQL, R, Tableau

## What This Project Actually Is
This is primarily a **SQL + Tableau demonstration**. The workflow is:

1. SQL aggregates raw transactional data into summary tables (monthly revenue, region performance, subcategory breakdowns)
2. R adds lag features, rolling averages, and a simple linear regression to generate forecast and residual outputs as CSVs
3. Tableau consumes those CSVs and presents the findings in an interactive dashboard

The R modeling layer is relatively thin — a lagged regression rather than a proper time-series model — and the forecast degrades quickly (the 3-month-out predictions drop to ~$25K against a baseline of ~$80–100K monthly revenue, which is not reliable). The main value of the R step is producing residual and decision classification columns that make the Tableau views more interesting.

## Methodology
- Used SQL to generate aggregated tables (monthly revenue, category, region, and subcategory performance)
- Engineered basic time-series features in R: lag-1, lag-2, and 3-month rolling average
- Fit a linear regression on lagged features to forecast next-month revenue
- Evaluated against a naive lag-1 baseline using MAE, RMSE, and MAPE
- Developed Tableau dashboards to visualize trends, seasonality, product performance, and decision outputs

## Model Performance (Honest Assessment)
| Model | MAE | RMSE | MAPE |
|---|---|---|---|
| Naive Lag-1 Baseline | $20,371 | $24,416 | 31.1% |
| Regression with Lag Features | $12,715 | $16,029 | 18.2% |

The regression model does beat the naive baseline, which is the minimum bar. However, an 18% MAPE on monthly retail revenue is a wide error margin, and the multi-step forecast deteriorates significantly by month 2–3. This model would not be used for actual planning decisions — it demonstrates the pipeline rather than delivering production-ready forecasts.

## Key Insights
- West region generated ~31% of total revenue with the lowest coefficient of variation (0.55) — the most stable, high-performing region
- South contributed ~17% with high volatility relative to revenue (CV: 0.76) — underperforming
- Phones (~$328K) and Chairs (~$323K) were the top revenue subcategories
- Fasteners (~$3K) and Labels had negligible contribution across the entire period
- Revenue shows clear seasonality with peaks in September and November; January and February are consistently soft

## Business Decisions (From Residual Classification)
These are derived from model residuals and relative performance — directionally useful but should be treated as conversation starters, not hard recommendations given the model's error rate.

- **Invest**: West region; Chairs, Phones, Storage, Tables, Accessories (positive average residuals, decent revenue share)
- **Monitor**: East region; Machines, Copiers, Binders (moderate revenue, high volatility)
- **Fix**: South and Central regions
- **Cut / Rework**: Bookcases, Appliances, Furnishings, Paper (low residuals, low share)

## Limitations
- The regression model uses only lag features — no external variables, no seasonality terms, no proper train/test split across time
- Multi-step forecasting reuses predicted values as inputs, compounding error quickly
- "Business decisions" are rule-based classifications on residuals, not the output of a decision model
- The dataset (Superstore) is a well-known sample dataset; results are illustrative

## Repository Structure
- `/data` — cleaned datasets used for analysis
- `/sql` — aggregation and KPI queries
- `/R` — feature engineering, modeling, and forecasting pipeline
- `/tableau` — dashboard workbook
- `/images` — dashboard screenshots
- `/results` — model outputs and decision tables