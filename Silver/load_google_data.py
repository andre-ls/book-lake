import os
import requests
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from BookDataLoader import BookDataLoader
from Schemas.SilverSchemas import googleSchema, volumeSchema

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Google Books Data Load").getOrCreate()

def getOpenLibraryData():
    return spark.read.parquet("DATA_DIRECTORY" + "/Bronze/books")

def requestGoogleData(title,isbn):
    url = 'https://www.googleapis.com/books/v1/volumes?q=' + title + '+isbn:' + isbn
    loader = BookDataLoader(url, googleSchema)
    return loader.getData()

df = requestGoogleData("O mundo de Sofia","9788571644755")
df.show(vertical=True,truncate=False)
df.printSchema()
