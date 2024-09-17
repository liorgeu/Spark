
"""
cume_dist
rcume_dist() window function is used to get the cumulative distribution
of values within a window partition.

This is the same as the DENSE_RANK function in SQL.
"""
from pyspark.sql.functions import cume_dist
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100), ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000), ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100)  )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
# df.printSchema()
# df.show(truncate=False)

windowSpec = Window.partitionBy("department").orderBy("salary")
df.withColumn("cume_dist",cume_dist().over(windowSpec)) \
   .show()