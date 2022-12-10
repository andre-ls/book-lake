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

def getBookReferences():
    bookReferences = spark.read.parquet(dataDirectory + "/Bronze/books")
    bookReferences = bookReferences.withColumn("isbn",f.when(bookReferences.isbn_13.isNotNull(),df.isbn_13)\
                             .otherwise(df.isbn_10))
    return bookReferences.select("title","isbn")

def requestGoogleData(title,isbn):
    url = 'https://www.googleapis.com/books/v1/volumes?q=' + title + '+isbn:' + isbn
    loader = BookDataLoader(url, googleSchema)
    return loader.getData()

def saveData(data):
    bookData = data.withColumn("book",f.explode("items")).select("book.*")
    bookData.write.mode("append").parquet("DATA_DIRECTORY + /Silver/googleBooks")

def getBookData(book):
    data = requestGoogleData(book.title,book.isbn[0])
    saveData(data)

def getGoogleData(bookReferences):
    bookReferences.foreach(lambda book: getBookData(book))

#df = requestGoogleData("O mundo de Sofia","9788571644755")

bookReferences = getBookReferences()
getGoogleData(bookReferences)
