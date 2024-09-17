'''
How many words are in the book?
'''
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# Read a text file
text = sc\
    .textFile(r"melville-moby_dick.txt")
print(text.take(30))

# We wish to clean all the non-letter characters using map(),
# so we write an auxiliary function called clean_word.
def clean_word(word):
    return ''.join([ch for ch in word if ch.isalpha()])

# We want to make an RDD of separate words,
# so we will go through the several processing steps.
# First, we split the lines into words using flatMap().

words_rdd = text\
    .flatMap(lambda line: line.split())
print(words_rdd.take(30))

words_rdd_clean = text\
    .flatMap(lambda line: line.split())\
    .map(clean_word)
print(words_rdd_clean.take(30))


words_rdd_clean_w = text\
    .flatMap(lambda line: line.split())\
    .map(clean_word)\
    .filter(len) # The filtering with len utilizes the fact that when checking an integer, then Python regards 0 as False and anything else as True.
print(words_rdd_clean_w.take(30))

# We can apply the action count() to simply find the number of elements in the RDD words.
print(words_rdd.count())