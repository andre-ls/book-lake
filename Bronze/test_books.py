from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, functions as f

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Book Test").getOrCreate()

df = spark.read.option("inferSchema","true").parquet(dataDirectory + "/Bronze/books")
#df = spark.read.option("inferSchema","true").text(dataDirectory + "/History/ol_dump_ratings_2022-10-31.txt")


df = df.withColumn("isbn",f.when(df.isbn_13.isNotNull(),df.isbn_13)\
                             .otherwise(df.isbn_10))
df.where(f.size(f.col("isbn")) > 0).show(vertical=True)

#df.printSchema()
#df.show(truncate=False)
#print("Total Rows: " + str(df.count()))
#df.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in df.columns]).show(vertical=True,truncate=False)
#print(df.where(df.isbn_10.isNull() & df.isbn_13.isNull()).count())
