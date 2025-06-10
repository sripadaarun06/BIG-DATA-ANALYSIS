# BIG-DATA-ANALYSIS-PySpark
PySpark script analyzing financial data: industry sales, expenses, and profits; YoY sales growth for Forestry &amp; Logging (2016–2025); top 5 industries by 2025 profit.

# Overview
This project analyzes a large financial dataset using PySpark to demonstrate scalability. The dataset contains sales and operating profit data for New Zealand industries (2016–2025).

# Requirements
- Python 3.8+
- PySpark 3.5.0
- Java 8 or 11
- CSV file: `business-financial-data-march-2025-quarter-csv.csv`

# Setup
1. Install dependencies: `pip install pyspark`
2. Place the CSV file in the project directory.
3. Run the script: `python business_financial_analysis.py`

# Analyses Performed
1. **Industry Yearly Summary**: Total and average sales/profit by industry and year.
2. **YoY Sales Growth**: Year-over-year sales growth for Forestry and Logging.
3. **Top Industries 2025**: Top 5 industries by operating profit in 2025.

# Insights
- Forestry and Logging sales peaked in 2021 ($1826.73M) but declined in 2025 ($1340M).
- Health Care and Social Assistance led in operating profit in 2025 (~$1221.23M).
- Parquet outputs ensure scalability for larger datasets.

# Output
Results are saved in Parquet format:
- `industry_yearly_summary.parquet`
- `forestry_sales_yoy.parquet`
- `top_industries_2025.parquet`
