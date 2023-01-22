import os
import requests
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType, StructType, ArrayType
from Schemas.SilverSchemas import googleSchema 

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Google Books Data Transform").getOrCreate()
sparkContext = spark.sparkContext

path = dataDirectory + "/Silver/googleBooksRaw"

def readData(path):
    data = spark.read.schema(googleSchema).json(path)
    return data

def processGoogleData(data):
    data = data.where(f.col("items").isNotNull())
    data = data.withColumn("items",f.col("items").getItem(0)).select("isbn","items.*")
    data = flat_data(data)
    data = filterColumns(data)
    data = renameColumns(data)
    return data

def filterColumns(data):
    selected_columns = [
            "isbn",
            "selfLink",
            "`volumeInfo.title`",
            "`volumeInfo.subtitle`",
            "`volumeInfo.authors`",
            "`volumeInfo.publisher`",
            "`volumeInfo.description`",
            "`volumeInfo.industryIdentifiers.type`",
            "`volumeInfo.industryIdentifiers.identifier`",
            "`volumeInfo.pageCount`",
            "`volumeInfo.categories`",
            "`volumeInfo.averageRating`",
            "`volumeInfo.ratingsCount`",
            "`volumeInfo.imageLinks.thumbnail`",
            "`volumeInfo.language`",
            "`volumeInfo.previewLink`",
            "`volumeInfo.infoLink`",
            "`volumeInfo.canonicalVolumeLink`"
        ]

    return data.select(selected_columns)

def renameColumns(data):
    renamed_columns = [
            "isbn",
            "selfLink",
            "title",
            "subtitle",
            "authors",
            "publisher",
            "description",
            "industry_identifiers_type",
            "industry_identifiers_values",
            "page_count",
            "categories",
            "average_rating",
            "ratings_count",
            "thumbnail_link",
            "language",
            "previewLink",
            "infoLink",
            "canonicalVolumeLink"
        ]

    return data.toDF(*renamed_columns)

def flat_data(data):
    columns = flatten(data.schema)
    data = data.select(columns)
    return data.toDF(*columns)

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

def saveData(df):
    df.write.mode("overwrite").parquet(dataDirectory + "/Silver/googleBooks")

df = readData(path)
df = processGoogleData(df)
saveData(df)

df.printSchema()
df.show(n=5,vertical=True)
print(">> Total Rows:" + str(df.count()))
