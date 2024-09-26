from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a Spark session
spark = SparkSession \
    .builder \
    .master("local")\
    .appName('color_table') \
    .getOrCreate()

# Define the schema for the color DataFrame
color_schema = StructType([
    StructField("color_id", IntegerType(), True),
    StructField("color_name", StringType(), True)
])

# Define the data for the color DataFrame
color_data = [
    (1, "Black"),
    (2, "Red"),
    (3, "Gray"),
    (4, "White"),
    (5, "Green"),
    (6, "Blue"),
    (7, "Pink")
]

# Create the color DataFrame
color_df = spark.createDataFrame(color_data, schema=color_schema)

# # Show the DataFrame
# color_df.show()
# color_df.printSchema()

# Write to S3
color_df.write.parquet('s3a://spark/data/car_colors', mode='overwrite')
spark.stop()

