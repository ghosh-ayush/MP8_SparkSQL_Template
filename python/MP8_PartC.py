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
# 3. Filtering (10 points) Count the number of appearances of word 'ATTRIBUTE'
####

# RDD api
# numAttr = rowrdd.filter(lambda x: x[0] == "ATTRIBUTE")
# print(numAttr.count())
# 201

# Spark SQL
query = "SELECT count(*) FROM gbooks WHERE word = 'ATTRIBUTE'"
sqlContext.sql(query).show()
# +--------+                                                                      
# |count(1)|
# +--------+
# |     201|
# +--------+


