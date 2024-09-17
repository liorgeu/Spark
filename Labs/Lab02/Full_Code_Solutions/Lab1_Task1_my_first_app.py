from pyspark import SparkContext

sc = SparkContext('local[*]', 'ex1_word_count')

text = """This is example of spark application in Python
Python is very common development language and it also one of Spark supported languages
The library of Spark in Python called PySpark
In this example you will implements word count application using PySpark
Good luck!!""".split("\n")

rdd = sc.parallelize(text)

flatted_words = rdd.flatMap(lambda line: line.split(' '))

words_with_rank = flatted_words.map(lambda word: (word, 1))

words_count = words_with_rank.reduceByKey(lambda a, b: a + b)

for word_count in words_count.collect():
    print(word_count)

sc.stop()
