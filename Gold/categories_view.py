import os
import requests
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
awsAccessKey = os.environ.get('AWS_ACCESS_KEY')
awsAccessSecret = os.environ.get('AWS_ACCESS_SECRET')
awsS3Directory = os.environ.get('AWS_S3_DIRECTORY')

spark = SparkSession.builder.appName("Categories View").getOrCreate()

spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey)
spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsAccessSecret)
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def readData():
    path = awsS3Directory + "/Silver/googleBooks"
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

def saveData(df):
    df.write.mode("overwrite").parquet(awsS3Directory + "/Gold/categoriesView")

df = readData()
df = processData(df)
saveData(df)
df.show(n=5,vertical=True,truncate=False)
