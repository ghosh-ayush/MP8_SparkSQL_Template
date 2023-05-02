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
sqlContext.setConf("spark.sql.broadcastTimeout", "3000")
####
# 5. Joining (10 points): The following program construct a new dataframe out of 'df' with a much smaller size.
####

df2 = df.select("word", "frequency").distinct().limit(100);
df2.createOrReplaceTempView('gbooks2')

# Now we are going to perform a JOIN operation on 'df2'. Do a self-join on 'df2' in lines with the same #'count1' values and see how many lines this JOIN could produce. Answer this question via DataFrame API and #Spark SQL API
# Dataframe API
#df2.alias("df1").join(df2.alias("df2"), on='count1').count()
# output: 9658

# Spark SQL API
query = "SELECT * FROM gbooks2 t1, gbooks2 t2 WHERE t1.year = t2.year"
# query = "SELECT * FROM gbooks2 a INNER JOIN gbooks2 b ON a.count1 = b.count1"
print(sqlContext.sql(query).count())
# output: 218

