import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType, ArrayType
from Schemas.book_schemas import editionSchema

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
awsAccessKey = os.environ.get('AWS_ACCESS_KEY')
awsAccessSecret = os.environ.get('AWS_ACCESS_SECRET')
awsS3Directory = os.environ.get('AWS_S3_DIRECTORY')

spark = SparkSession.builder.appName("Book Historical Load").getOrCreate()

spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsAccessKey)
spark.sparkContext\
     ._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsAccessSecret)
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext\
      ._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.option("inferSchema","true").text(dataDirectory + "/History/ol_dump_editions_2022-10-31.txt")

@f.udf(returnType = StringType())
def cleanRow(row):
    if '{' in row:
        row = row[row.find('{'):]
        return re.sub("/[{}]|(\/\w*\/)","",row)

def extractJsonData(df):
    cleaned_df = df.select(cleanRow(f.col('value')).alias("book"))
    return cleaned_df.withColumn("jsonData",f.from_json(f.col("book"),editionSchema)).select("jsonData.*")

def removeBooksWithoutIsbn(df):
    df = df.where(df.isbn_10.isNotNull() | df.isbn_13.isNotNull())
    return df.where((f.size(f.col("isbn_10")) > 0) | (f.size(f.col("isbn_13")) > 0))

def formatIsbnColumns(df):
    df = df.withColumn("isbn_10",f.regexp_replace(f.col("isbn_10").getItem(0), "[^\dX+]", ""))
    df = df.withColumn("isbn_13",f.regexp_replace(f.col("isbn_13").getItem(0), "[^\dX+]", ""))
    return df.withColumn("isbn",f.when(df.isbn_13.isNotNull(),df.isbn_13)\
                                 .otherwise(df.isbn_10))

def filterColumns(df):
    return df.select("key","title","isbn_10","isbn_13","isbn")

df = extractJsonData(df)
df = removeBooksWithoutIsbn(df)
df = formatIsbnColumns(df)
df = filterColumns(df)

df.write.mode("overwrite").parquet(awsS3Directory + "/Bronze/books")

