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
    # Columns: 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)
    rdd_parsed = rdd.map(lambda line: line.split("\t")).map(
        lambda tokens: (tokens[0], int(tokens[1]), int(tokens[2]), int(tokens[3]))
    )
    
    # Define the schema for the DataFrame.
    schema = StructType([
        StructField("word", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("frequency", IntegerType(), True),
        StructField("books", IntegerType(), True)
    ])
    
    # Create a DataFrame using the parsed RDD and the defined schema.
    df = spark.createDataFrame(rdd_parsed, schema)
    return df

# Load the data from the "gbooks" file (assumed to be in the current directory)
df = load_data("gbooks")
####
# 5. Joining : The following program construct a new dataframe out of 'df' with a much smaller size.
####

df2 = df.select("word", "year").distinct().orderBy("year", "word").limit(100)
df2.createOrReplaceTempView('gbooks2')

# Now we are going to perform a JOIN operation on 'df2'. Do a self-join on 'df2' in lines with the same #'count1' values and see how many lines this JOIN could produce. Answer this question via Spark SQL API

# Spark SQL API

# output: 310
result = spark.sql("""
    SELECT COUNT(*) as count
    FROM gbooks2 a
    JOIN gbooks2 b
    ON a.year = b.year
""")
result_count = result.collect()[0]["count"]
print(result_count)