from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

sc = SparkContext()
sqlContext = SQLContext(sc)

schema = StructType([       
    StructField('word', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('frequency', IntegerType(), True),
    StructField('books', IntegerType(), True),
])

####
# 1. Setup (10 points): Download the gbook file and write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)
def parseLine(line):
    line = line.split('\t')
    line = (line[0], int(line[1]), int(line[2]), int(line[3]))
    return line

def getRDD(filename):
    textFile = sc.textFile(filename) # make sure the gbook file is in the CURRENT file path
    parsedRDD = textFile.map(parseLine)
    return parsedRDD
    
rowrdd = getRDD("gbooks").cache()

# Spark SQL - DataFrame API
df = sqlContext.createDataFrame(rowrdd, schema)
df.createOrReplaceTempView('gbooks') # Register table name for SQL

####
# 4. MapReduce (10 points): List the three most frequent 'word' with their count of appearances
####

# RDD API
#rowrdd.map(lambda q : (q[0], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda k, v: v, False).take(3)
# [('at_ADP', 425), ('all', 425), ('about', 425)]

# Spark SQL
query = "SELECT DISTINCT word, count(*) FROM gbooks WHERE word is not null GROUP BY word"
sqlContext.sql(query).orderBy("count(1)", ascending=False).show(3) # There are 18 items with count = 425, so could be different 
# +---------+--------+
# |     word|count(1)|
# +---------+--------+
# |  all_DET|     425|
# | are_VERB|     425|
# |about_ADP|     425|
# +---------+--------+

