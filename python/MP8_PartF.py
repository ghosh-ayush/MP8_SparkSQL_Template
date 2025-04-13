from pyspark.sql.functions import col, lag, when, sum as _sum
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

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
# Filter data to include only records from year 1500 up to and including 2000.
df_filtered = df.filter((col("year") >= 1500) & (col("year") <= 2000))

# Define a window specification partitioned by "word" and ordered by "year".
windowSpec = Window.partitionBy("word").orderBy("year")

# Compute the previous frequency for each word using lag.
df_with_lag = df_filtered.withColumn("prev_frequency", lag("frequency").over(windowSpec))

# Calculate the frequency difference between the current row and the previous row.
# For the first occurrence, lag returns null so we substitute 0.
df_with_diff = df_with_lag.withColumn("freq_diff",
    when(col("prev_frequency").isNull(), 0)
    .otherwise(col("frequency") - col("prev_frequency"))
)

# Ensure that any negative difference is treated as 0 (only count increases).
df_with_diff = df_with_diff.withColumn("freq_diff",
    when(col("freq_diff") < 0, 0).otherwise(col("freq_diff"))
)

# For each word, sum up the computed differences to get the total increase.
df_word_increase = df_with_diff.groupBy("word").agg(
    _sum("freq_diff").alias("total_increase")
)

# Order the results by total_increase in descending order and display the top 20 rows.
df_word_increase.orderBy(col("total_increase").desc()).show(20)