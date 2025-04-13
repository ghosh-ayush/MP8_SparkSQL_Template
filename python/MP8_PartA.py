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


# Spark SQL - DataFrame API

def load_data(filepath):
    # Load file as an RDD of lines.
    rdd = sc.textFile(filepath)
    
    # Parse each line into a tuple:
    # Column 0: word (string)
    # Column 1: year (int)
    # Column 2: frequency (int)
    # Column 3: books (int)
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
    
    # Create a DataFrame using the parsed RDD and the defined schema
    df = spark.createDataFrame(rdd_parsed, schema)
    
    return rdd_parsed, df

# Load the data from the "gbooks" file (assumed to be in the current directory)
rdd_data, df_data = load_data("gbooks")
df_data.printSchema()

