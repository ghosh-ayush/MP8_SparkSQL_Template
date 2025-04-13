from pyspark.sql.functions import col, lag, when, sum as _sum
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F

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
    
    # Create and return a DataFrame using the parsed RDD.
    df = spark.createDataFrame(rdd_parsed, schema)
    return df

# Load the data from "gbooks" (assumed to be in the current directory)
df = load_data("gbooks")

###
# 2. Frequency Increase : analyze the frequency increase of words starting from the year 1500 to the year 2000
###
# Spark SQL - DataFrame API


# df_word_increase.show()
# Filter records to include only years in [1500, 2000]
df_filtered = df.filter((F.col("year") >= 1500) & (F.col("year") <= 2000))

# (a) Determine each word’s first occurrence (minimum year) and obtain its frequency.
first_occurrence = df_filtered.groupBy("word").agg(F.min("year").alias("first_year")).alias("first")
# Alias the filtered DataFrame to avoid ambiguity.
df_filtered_alias = df_filtered.alias("d")
first_freq = first_occurrence.join(
    df_filtered_alias,
    (F.col("first.word") == F.col("d.word")) & (F.col("first.first_year") == F.col("d.year"))
).select(
    F.col("d.word").alias("word"),
    F.col("d.frequency").alias("first_freq")
)

# (b) Compute the total frequency for each word (sum of frequency over all rows in [1500,2000]).
total_freq = df_filtered.groupBy("word").agg(F.sum("frequency").alias("total_freq"))

# (c) Calculate total_increase = total_freq - first_freq.
df_increase = first_freq.join(total_freq, on="word")\
    .withColumn("total_increase", F.col("total_freq") - F.col("first_freq"))

# (d) Select only "word" and "total_increase" and order by total_increase descending.
df_result = df_increase.select("word", "total_increase").orderBy(F.desc("total_increase"))

# 3. OUTPUT FORMAT: Produce output with correct alignment.
# We simply call show(5, False) to produce output where numbers remain numeric.
df_result.show(20)