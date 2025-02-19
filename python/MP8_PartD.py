from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)


# Spark SQL - DataFrame API

####
#  4. MapReduce : List the top three words that have appeared in the
#  greatest number of years. 
#  Sorting order of the final answer should should be descending by word count,
#  then descending by word.

# Spark SQL

# +-------------+--------+
# |         word|count(1)|
# +-------------+--------+
# |    ATTRIBUTE|      11|
# |approximation|       4|
# |    agast_ADV|       4|
# +-------------+--------+
# only showing top 3 rows

