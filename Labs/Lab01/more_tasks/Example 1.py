'''
Read a melville-moby_dick text file and print the first 15 lines.
'''
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# Read a text file
text = sc\
    .textFile(r"melville-moby_dick.txt")

# Take first 15 lines
print(text.take(15))