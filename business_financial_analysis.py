from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum, avg, lag, when, round
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("BusinessFinancialDataAnalysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

file_path = "C:/Users/Dell/Desktop/Mine/Himalayan/DATASET/business-financial-data-march-2025-quarter-csv.csv"
df = spark.read.option("header", "true").csv(file_path)

df = df.withColumn("Data_value", col("Data_value").cast("float")) \
       .withColumn("Period", col("Period").cast("float")) \
       .filter(col("Data_value").isNotNull()) \
       .filter(col("Period").isNotNull())

df = df.withColumn("Year", col("Period").cast("string").substr(1, 4).cast("int"))

df.cache()

industry_yearly_summary = df.filter(col("Series_title_4") == "Unadjusted") \
    .groupBy("Series_title_2", "Year", "Series_title_1") \
    .agg(
        sum("Data_value").alias("total_value"),
        avg("Data_value").alias("avg_value")
    ) \
    .withColumn("total_value", round(col("total_value"), 2)) \
    .withColumn("avg_value", round(col("avg_value"), 2))

forestry_sales = df.filter((col("Series_title_2") == "Forestry and Logging") & 
                          (col("Series_title_1") == "Sales (operating income)") & 
                          (col("Series_title_4") == "Unadjusted")) \
    .groupBy("Year") \
    .agg(sum("Data_value").alias("total_sales")) \
    .withColumn("prev_year_sales", lag(col("total_sales"), 1).over(Window.partitionBy().orderBy("Year"))) \
    .withColumn("yoy_growth_percent", 
                when(col("prev_year_sales").isNotNull(),
                     round(((col("total_sales") - col("prev_year_sales")) / col("prev_year_sales") * 100), 2)))

top_industries_2025 = df.filter((col("Year") == 2025) & 
                               (col("Series_title_1") == "Operating profit") & 
                               (col("Series_title_4") == "Unadjusted")) \
    .groupBy("Series_title_2") \
    .agg(sum("Data_value").alias("total_profit")) \
    .orderBy(col("total_profit").desc()) \
    .limit(5)

print("Industry Yearly Summary (Sales and Operating Profit):")
industry_yearly_summary.show(10, truncate=False)

print("Year-over-Year Sales Growth for Forestry and Logging:")
forestry_sales.show(truncate=False)

print("Top 5 Industries by Operating Profit in 2025:")
top_industries_2025.show(truncate=False)

industry_yearly_summary.write.mode("overwrite").parquet("industry_yearly_summary.parquet")
forestry_sales.write.mode("overwrite").parquet("forestry_sales_yoy.parquet")
top_industries_2025.write.mode("overwrite").parquet("top_industries_2025.parquet")

df.unpersist()
spark.stop()