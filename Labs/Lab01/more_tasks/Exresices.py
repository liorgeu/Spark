'''
 Read the file "english words" into an RDD and answer the following questions:

1. How many words are listed in the file?
2. What is the most common first letter?
3. What is the longest word in the file?
4. How many words include all 5 vowels?
'''

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# Read a text file
text = sc\
    .textFile(r"/tmp/pycharm_project_574/05_spark/spark_RDD/english words.txt")

print(text.take(10))

# 1. How many words are listed in the file?
print(text.count())

# 2. What is the most common first letter?
text_rdd2 = text\
    .map(lambda x: x.lower())\
    .groupBy(lambda word: word[0])\
    .mapValues(len)\
    .sortBy(lambda word:word[1], ascending= False)

print(text_rdd2.take(10))


#option 2
# text_rdd = text.map(lambda word: (word[0].lower(),1))\
#     .reduceByKey(lambda a,b:a+b)\
#     .sortBy(lambda word:word[1], ascending= False)
#
# print(text_rdd.take(10))



# 3. What is the longest word in the file?

word_len_max = text.map(lambda x: len(x)).max()
print(word_len_max)
longest_word = text.filter(lambda word: len(word)==word_len_max)
print(longest_word.take(60))

# 4. How many words include all 5 vowels?
text_rdd= text.filter(lambda word: set("aeiuo")<set(word))
print(text_rdd.take(10))









