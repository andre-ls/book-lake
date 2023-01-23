import os
import requests
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Categories View").getOrCreate()
sparkContext = spark.sparkContext

def readData():
    path = dataDirectory + "/Silver/googleBooks"
    data = spark.read.option("inferSchema","true").parquet(path)
    return data

def processData(data):
    data = data.withColumn("author",f.explode(f.col("authors")))
    data = data.withColumn("category",f.explode(f.col("categories")))
    data = data.groupBy("category").agg(f.avg("average_rating").alias("average_rating"),\
                                      f.sum("ratings_count").alias("total_ratings"),\
                                      f.count("isbn").alias("total_books"),\
                                      f.collect_set("title").alias("books_titles"),\
                                      f.collect_set("language").alias("languages"),\
                                      f.collect_set("author").alias("author"),\
                                      f.collect_set("publisher").alias("publisher"))
    data = data.where(f.col("category").isNotNull())
    return data

df = readData()
print(">> Total Rows: " + str(df.count()))
df = processData(df)
print(">> Total Rows: " + str(df.count()))
df.show(n=5,vertical=True,truncate=False)
