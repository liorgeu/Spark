

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

my_rdd = sc.parallelize([  ("a",1), ("a",2),("b",1), ("a",1), ("a",3), ("b",4)])
rdd_reduce = my_rdd.reduceByKey(lambda k1,k2: k1+k2)
print(rdd_reduce.take(2))
# [('a', 7), ('b', 5)]

my_rdd1 = sc.parallelize([  ("a",1), ("a",2),("b",1), ("a",1), ("a",3), ("b",4),("z",1), ("z",3), ("b",4)])
rdd_reduce1 = my_rdd1.reduceByKey(lambda k1,k2: k1+k2)
print(rdd_reduce1.take(7))
# [('a', 7), ('z', 4), ('b', 9)]

my_rdd2 = sc.parallelize([  ("avi",1), ("alin",-2),("avi",1), ("alin",1), ("niv",3),("niv",4),("niv",1)])
rdd_reduce2 = my_rdd2.reduceByKey(lambda k1,k2: k1+k2)
print(rdd_reduce2.take(7))
# [('avi', 2), ('alin', -1), ('niv', 8)]

