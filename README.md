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

#How to Run
Navigate to the Directory:
-Open PowerShell and change to the script’s directory:

cd 'C:/ File Path/'
Run the Script:

Execute the script:
python business_financial_analysis.py

#Expected Output:

The script will display:
A table summarizing financial metrics by industry and year.
A table showing YoY sales growth for the Forestry and Logging industry (2016–2025).
A table listing the top 5 industries by operating profit in 2025.

Example output snippet:
+---------------------------------------------------------------------------+----+-----------------------------------+-----------+---------+
|Series_title_2                                                             |Year|Series_title_1                     |total_value|avg_value|
+---------------------------------------------------------------------------+----+-----------------------------------+-----------+---------+
|Agriculture, Forestry and Fishing                                          |2024|Sales (operating income)           |14185.78   |3546.45  |
|Wholesale Trade                                                            |2023|Sales (operating income)           |307001.48  |38375.18 |
|Transport Equipment, Machinery and Equipment Manufacturing                 |2017|Purchases and operating expenditure|7392.01    |1848.0   |
|Retail Trade                                                               |2021|Purchases and operating expenditure|77411.5    |19352.88 |
|Transport, Postal and Warehousing                                          |2017|Purchases and operating expenditure|35452.14   |4431.52  |
|Accommodation and Food Services                                            |2024|Salaries and wages                 |6438.17    |1609.54  |
|Other Services                                                             |2017|Salaries and wages                 |2385.65    |596.41   |
|Fishing, Aquaculture and Agriculture, Forestry and Fishing Support Services|2016|Operating profit                   |540.73     |180.24   |
|Arts, Recreation and Other Services                                        |2020|Purchases and operating expenditure|8751.88    |2187.97  |
|Petroleum, Chemical, Polymer and Rubber Product Manufacturing              |2024|Salaries and wages                 |1781.8     |445.45   |
+---------------------------------------------------------------------------+----+-----------------------------------+-----------+---------+

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
