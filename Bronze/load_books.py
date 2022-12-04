import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType, ArrayType
from Schemas.book_schemas import editionSchema

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Book Historical Load").getOrCreate()

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
    return df.where(df.isbn_10.isNotNull() | df.isbn_13.isNotNull())

def removeEmptyColumns(df):
    return df.drop('ocaid','links','weight','edition_name','physical_dimensions','genres','work_titles','table_of_contents','description','first_sentence')

def extractSingleValueColumns(df):
    df = df.withColumn("type",f.col("type.key"))
    df = df.withColumn("created",f.col("created.value"))
    df = df.withColumn("last_modified",f.col("last_modified.value"))
    df = df.withColumn("authors",f.col("authors.key"))
    df = df.withColumn("works",f.col("works.key"))
    df = df.withColumn("languages",f.col("languages.key"))

    return df

df = extractJsonData(df)
df = removeBooksWithoutIsbn(df)
df = removeEmptyColumns(df)
df = extractSingleValueColumns(df)

df.write.option("mode","overwrite").parquet(dataDirectory + "/Bronze/books")

