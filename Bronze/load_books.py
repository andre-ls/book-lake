from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType,StringType
from Schemas.book_schemas import editionSchema
import json

spark = SparkSession.builder.appName("Book Test").getOrCreate()

df = spark.read.option("inferSchema","true").text("Data/History/ol_dump_editions_2022-10-31.txt")

@f.udf(returnType = StringType())
def cleanRow(row):
    if '{' in row:
        return row[row.find('{'):]

cleaned_df = df.select(cleanRow(f.col('value')).alias("book"))
json_df = cleaned_df.withColumn("jsonData",f.from_json(f.col("book"),editionSchema)).select("jsonData.*")

json_df.write.option("mode","overwrite").parquet("Data/Bronze/books")

