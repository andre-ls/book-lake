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
    bookReferences = spark.read.parquet(dataDirectory + "/Bronze/books")
    bookReferences = bookReferences.withColumn("isbn",f.when(bookReferences.isbn_13.isNotNull(),bookReferences.isbn_13)\
                             .otherwise(bookReferences.isbn_10))
    return bookReferences.select("title","isbn")

@f.udf(returnType = StringType())
def requestGoogleData(isbn):
    url = 'https://www.googleapis.com/books/v1/volumes?q=isbn:' + isbn
    response = requests.get(url).json()
    return json.dumps(response)

def saveData(data):
    bookData = data.withColumn("book",f.explode("items")).select("book.*")
    bookData.write.mode("append").parquet("DATA_DIRECTORY + /Silver/googleBooks")

def getGoogleData(bookReferences):
    data = bookReferences.withColumn("data",requestGoogleData(f.col("isbn").getItem(0)))
    return data.withColumn("book",f.from_json("data",googleSchema)).drop("data")

def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

#df = requestGoogleData("9788571644755")

bookReferences = getBookReferences()
bookReferences = bookReferences.limit(5)
df = getGoogleData(bookReferences)
df = df.withColumn("book",f.explode("book.items"))

df = df.select(flatten(df.schema))

df.printSchema()
df.show(n=5,vertical=True,truncate=False)
