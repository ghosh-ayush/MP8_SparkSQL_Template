from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)
def load_data(filepath):
    # Load file as an RDD of lines.
    rdd = sc.textFile(filepath)
    
    # Parse each line into a tuple with columns:
    # 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)
    rdd_parsed = rdd.map(lambda line: line.split("\t")).map(
        lambda tokens: (tokens[0], int(tokens[1]), int(tokens[2]), int(tokens[3]))
    )
    
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("word", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("frequency", IntegerType(), True),
        StructField("books", IntegerType(), True)
    ])
    
    # Create the DataFrame using the parsed RDD and the defined schema
    df = spark.createDataFrame(rdd_parsed, schema)
    
    return rdd_parsed, df

# Load the data (assuming the "gbooks" file is in the current directory)
rdd_data, df_data = load_data("gbooks")

# Spark SQL - DataFrame API

####
# 2. Counting : How many lines does the file contains? Answer this question via both RDD api & #Spark SQL
####

# Spark SQL 

# sqlContext.sql(query).show() or df.show()
# +--------+
# |count(1)|
# +--------+
# |   50013|
# +--------+
rdd_line_count = rdd_data.count()
#print("RDD line count:", rdd_line_count)

# Using Spark SQL / DataFrame API to count the number of lines
# Register the DataFrame as a temporary view for SQL queries
df_data.createOrReplaceTempView("gbooks")

# Execute the SQL query to count the number of rows
sql_df = spark.sql("SELECT COUNT(*) as `count(1)` FROM gbooks")
sql_df.show()

