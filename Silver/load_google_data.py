import os
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Google Books Data Load").getOrCreate()

def getOpenLibraryData():
    return spark.read.parquet("DATA_DIRECTORY" + "/Bronze/books")

def requestGoogleData(title,isbn):
    return requests.get('https://www.googleapis.com/books/v1/volumes?q=' + title + '+isbn:' + isbn).json()

json = requestGoogleData("O mundo de Sofia","9788571644755")
