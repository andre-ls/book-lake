import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType
from Schemas.book_schemas import authorSchema

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Load Authors").getOrCreate()

df = spark.read.option("inferSchema","true").text(dataDirectory + "/History/ol_dump_authors_2022-10-31.txt")

@f.udf(returnType = StringType())
def cleanRow(row):
    if '{' in row:
        row = row[row.find('{'):]
        return re.sub("/[{}]|(\/\w*\/)","",row)

def extractJsonData(df):
    cleaned_df = df.select(cleanRow(f.col('value')).alias("author"))
    return cleaned_df.withColumn("jsonData",f.from_json(f.col("author"),authorSchema)).select("jsonData.*")

def filterColumns(df):
    return df.select("key","name","type","personal_name","revision","created","last_modified")

def extractSingleValueColumns(df):
    df = df.withColumn("type",f.col("type.key"))
    df = df.withColumn("created",f.col("created.value"))
    df = df.withColumn("last_modified",f.col("last_modified.value"))

    return df

df = extractJsonData(df)
df = filterColumns(df)
df = extractSingleValueColumns(df)

df.write.option("mode","overwrite").parquet(dataDirectory + "/Bronze/authors")
