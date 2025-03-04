from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ParquetToHBase") \
    .getOrCreate()

# Load Parquet Data
df = spark.read.parquet("hdfs://localhost:9000/weather_data")
df.show()
