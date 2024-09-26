from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a Spark session
spark = SparkSession \
    .builder \
    .master("local")\
    .appName('model_table') \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("model_id", IntegerType(), True),
    StructField("car_brand", StringType(), True),
    StructField("car_model", StringType(), True)
])

# Define the data
data = [
    (1, "Mazda", "3"),
    (2, "Mazda", "6"),
    (3, "Toyota", "Corolla"),
    (4, "Hyundai", "i20"),
    (5, "Kia", "Sportage"),
    (6, "Kia", "Rio"),
    (7, "Kia", "Picanto")
]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# # Show the DataFrame
# df.show()
# df.printSchema()

# Write to S3
df.write.parquet('s3a://spark/data/car_models', mode='overwrite')
spark.stop()





