from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql.functions import expr

# Create a Spark session
spark = SparkSession \
    .builder \
    .master("local") \
    .appName('cars_generator') \
    .getOrCreate()

# Generate 20 cars based on the specified data logic
num_cars = 20

# Define the schema for the car DataFrame
car_schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("driver_id", LongType(), True),
    StructField("model_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True)
])

# Create the car DataFrame with random data
car_df = spark.range(num_cars) \
    .withColumn("car_id", expr("CAST(rand() * 9000000 + 1000000 AS INT)")) \
    .withColumn("driver_id", expr("CAST(rand() * 900000000 + 100000000 AS LONG)")) \
    .withColumn("model_id", expr("CAST(rand() * 6 + 1 AS INT)")) \
    .withColumn("color_id", expr("CAST(rand() * 6 + 1 AS INT)")) \
    .select("car_id", "driver_id", "model_id", "color_id")

# # Show the car DataFrame
car_df.show()
# car_df.printSchema()

# Write the car DataFrame to Parquet format
car_df.write.parquet('s3a://spark/data/cars', mode='overwrite')

# Stop the Spark session
spark.stop()
