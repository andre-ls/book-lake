from dotenv import load_dotenv
import os
import json
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType,StringType
from Schemas.book_schemas import authorSchema

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Load Authors").getOrCreate()

df = spark.read.option("inferSchema","true").text(dataDirectory + "/History/ol_dump_authors_2022-10-31.txt")

@f.udf(returnType = StringType())
def cleanRow(row):
    if '{' in row:
        return row[row.find('{'):]

cleaned_df = df.select(cleanRow(f.col('value')).alias("author"))
json_df = cleaned_df.withColumn("jsonData",f.from_json(f.col("author"),authorSchema)).select("jsonData.*")

json_df.write.option("mode","overwrite").parquet(dataDirectory + "/Bronze/authors")
