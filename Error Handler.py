try:
    df = spark.read.option("header", "true").csv(file_path)
except Exception as e:
    print(f"Error loading file: {e}")
    spark.stop()
    exit(1)