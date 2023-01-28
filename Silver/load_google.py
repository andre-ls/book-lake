import os
import requests
import json
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Row, functions as f 
from pyspark.sql.types import StringType

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
awsAccessKey = os.environ.get('AWS_ACCESS_KEY')
awsAccessSecret = os.environ.get('AWS_ACCESS_SECRET')
awsS3Directory = os.environ.get('AWS_S3_DIRECTORY')

spark = SparkSession.builder.appName("Google Books Data Load").getOrCreate()

spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey)
spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsAccessSecret)
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def getBookReferences():
    bookHistory = spark.read.parquet(awsS3Directory + "/Bronze/books")
    try:
        bookReferences = spark.read.json(awsS3Directory + "/Silver/googleBooksRaw")
        print(">>Requested Book History found")
        bookReferences = bookHistory.join(bookReferences,"isbn",how="left").filter("kind is null").select("isbn")
        print(">>Books to Load:" + str(bookReferences.count()))
        return bookReferences
    except:
        print(">>Requested Book History not found")
        print(">>Books to Load:" + str(bookHistory.count()))
        return bookHistory.select("isbn")

def requestGoogleData(isbn):
    url = 'https://www.googleapis.com/books/v1/volumes?q=isbn:' + str(isbn)
    response = requests.get(url).json()
    response['isbn'] = isbn
    return response

def saveData(isbn,data,s3Client):

    print(">>Loading ISBN: " + str(isbn))
    s3Client.put_object(
         Body=json.dumps(data),
         Bucket='ohara-project',
         Key="Silver/googleBooksRaw/" + str(isbn) + ".json" 
    )

def getAndSaveGoogleData(dataPartition):

    s3Client = boto3.client(
        's3',
        aws_access_key_id=awsAccessKey,
        aws_secret_access_key=awsAccessSecret
    )
    print(">>AWS Connection Created")

    for row in dataPartition:
        try:
            jsonData = requestGoogleData(row.isbn)
            saveData(row.isbn,jsonData,s3Client)
            yield Row(isbn=row.isbn,result="OK")
        except:
            yield Row(isbn=row.isbn,result="Error")

bookReferences = getBookReferences()
booksRdd = bookReferences.rdd.repartition(spark.sparkContext.defaultParallelism)
results = booksRdd.mapPartitions(getAndSaveGoogleData).toDF()
results.show(truncate=False)
