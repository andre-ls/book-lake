from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, functions as f

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Google Book Test Read").getOrCreate()

df = spark.read.option("inferSchema","true").parquet(dataDirectory + "/Silver/googleBooks")

df.printSchema()
df.show(truncate=False,vertical=True)
print("Total Rows: " + str(df.count()))
#df.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in df.columns]).show(vertical=True,truncate=False)
#print(df.where(df.isbn_10.isNull() & df.isbn_13.isNull()).count())
