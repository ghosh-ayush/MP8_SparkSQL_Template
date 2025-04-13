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
def load_data(filepath):
    # Load the file as an RDD of lines
    rdd = sc.textFile(filepath)
    
    # Parse each line into a tuple:
    # 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)
    rdd_parsed = rdd.map(lambda line: line.split("\t")).map(
        lambda tokens: (tokens[0], int(tokens[1]), int(tokens[2]), int(tokens[3]))
    )
    
    # Define a schema for the DataFrame
    schema = StructType([
        StructField("word", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("frequency", IntegerType(), True),
        StructField("books", IntegerType(), True)
    ])
    
    # Create a DataFrame with the parsed RDD and schema
    df = spark.createDataFrame(rdd_parsed, schema)
    return rdd_parsed, df

# Load the data from "gbooks" (assumed to be in the current directory)
rdd_data, df_data = load_data("gbooks")
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

df_data.createOrReplaceTempView("gbooks")

# Using Spark SQL to group by word and count its appearances.
# Sorting order: descending by count and descending by word.
result_df = spark.sql("""
    SELECT word, COUNT(*) as `count(1)` 
    FROM gbooks 
    GROUP BY word 
    ORDER BY `count(1)` DESC, word DESC 
""")
result_df.show(3)