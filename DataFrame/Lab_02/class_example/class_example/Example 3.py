'''
How many groups purchased a dessert?
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Read a csv file
dessert = spark.read.csv(r"dessert.csv",
                         header=True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

#################################################

dessert.show(5)
dessert.printSchema()

print(dessert.where(dessert.purchase).count())

print(dessert.filter(dessert.purchase).count())

'''
How many groups purchased a dessert on Mondays?
'''
(print(dessert.filter((dessert.purchase) & (dessert.weekday == "Monday") ).count()))

'''
How many visitors purchased a dessert?
What is the average table?
'''

dessert\
    .where(dessert.purchase)\
    .agg({'num_of_guests': 'sum', 'table': 'mean'})\
    .show()

