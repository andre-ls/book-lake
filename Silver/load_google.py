import os
import requests
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType, StructType, ArrayType
from Schemas.SilverSchemas import googleSchema, volumeSchema

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Google Books Data Load").getOrCreate()
sparkContext = spark.sparkContext

def getBookReferences():
    bookHistory = spark.read.parquet(dataDirectory + "/Bronze/books")
    try:
        bookReferences = spark.read.json(dataDirectory + "/Silver/googleBooks")
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

def saveData(isbn,data):
    path = dataDirectory + "/Silver/googleBooks/" + str(isbn) + ".json"
    print(">> ISBN:" + str(isbn))
    with open(path,'w') as f:
        json.dump(data,f)

@f.udf(returnType=StringType())
def getAndSaveGoogleData(isbn):
    try:
        jsonData = requestGoogleData(isbn)
        saveData(isbn,jsonData)
        return "OK"
    except:
        return "Error"

bookReferences = getBookReferences()
bookReferences = bookReferences.withColumn("requestStatus",getAndSaveGoogleData(f.col("isbn")))
bookReferences.show(truncate=False)
