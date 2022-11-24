import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Load Ratings").getOrCreate()

df = spark.read.option("inferSchema","true").text(dataDirectory + "/History/ol_dump_ratings_2022-10-31.txt")

split_df = df.withColumn('work_key',f.split(df['value'], '\t').getItem(0))\
             .withColumn('edition_key',f.split(df['value'], '\t').getItem(1))\
             .withColumn('rating',f.split(df['value'], '\t').getItem(2))\
             .withColumn('date',f.split(df['value'], '\t').getItem(3))

split_df = split_df.drop(split_df.value)

split_df.write.option("mode","overwrite").parquet(dataDirectory + "/Bronze/ratings")
