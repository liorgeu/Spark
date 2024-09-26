from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType


car_schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("driver_id", LongType(), True),
    StructField("model_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True)
])

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('data_generator')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()    

streaming_cars = spark.read\
    .schema(car_schema)\
    .parquet('s3a://spark/data/cars')

streaming_cars.cache()
    
while True:
    
    event = streaming_cars.select(F.col('car_id'))\
        .withColumn('event_id' ,F.concat(F.col('car_id'), F.unix_timestamp()))\
        .withColumn('event_time' , F.current_timestamp())\
        .withColumn('speed' , F.round(F.rand() * 200).cast("int"))\
        .withColumn('rpm' , F.round(F.rand() * 8000).cast("int"))\
        .withColumn('gear' ,  1+ F.round(F.rand() * 6).cast("int"))

    # # Show the car DataFrame    
    # event.show(truncate=False)
        
    kafka_writer = event.selectExpr("to_json(struct(*)) AS value")\
        .write\
        .format('kafka')\
        .option("kafka.bootstrap.servers", "course-kafka:9092")\
        .option("topic", "sensors-sample")\
        .option("checkpointLocation", "s3a://spark/checkpoints/final_ex/events")\
        .save()
        
    sleep(1)
                
     
