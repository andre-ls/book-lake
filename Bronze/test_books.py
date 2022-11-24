from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, functions as f

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Book Test").getOrCreate()

#df = spark.read.option("inferSchema","true").parquet(dataDirectory + "/Bronze/authors")
df = spark.read.option("inferSchema","true").text(dataDirectory + "/History/ol_dump_ratings_2022-10-31.txt")

df.printSchema()
df.show(truncate=False)
print("Total Rows: " + str(df.count()))
