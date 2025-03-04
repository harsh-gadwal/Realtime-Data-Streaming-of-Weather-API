from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema for the weather data
schema = StructType([
    StructField("City", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Temperature (F)", FloatType(), True),
    StructField("Feels Like (F)", FloatType(), True),
    StructField("Minimum Temp (F)", FloatType(), True),
    StructField("Maximum Temp (F)", FloatType(), True),
    StructField("Pressure", FloatType(), True),
    StructField("Humidity", FloatType(), True),
    StructField("Wind Speed", FloatType(), True),
    StructField("Time of Record", StringType(), True),
    StructField("Sunrise (Local Time)", StringType(), True),
    StructField("Sunset (Local Time)", StringType(), True)
])

def consume_weather_data_from_kafka():
    # Read the stream from Kafka topic 'weather_topic'
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather_topic") \
        .load()

    # Extract the message value from Kafka (which is in binary format)
    value_df = raw_df.selectExpr("CAST(value AS STRING)")

    # Convert the value column (JSON) to a structured format using the defined schema
    weather_df = value_df.select(from_json(col("value"), schema).alias("weather_data"))

    # Flatten the structure and select required columns
    final_df = weather_df.select("weather_data.*")

    # Write the streaming data to HDFS with checkpointing
    query = final_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "hdfs://localhost:9000/weather_data") \
        .option("checkpointLocation", "hdfs://localhost:9000/checkpoint/weather_data") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    try:
        consume_weather_data_from_kafka()
    except Exception as e:
        print(f"Error: {e}")
    spark.stop()
