from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, functions as f
from Schemas.book_schemas import editionSchema

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Book Test").getOrCreate()

df = spark.read.option("inferSchema","true").parquet(dataDirectory + "/Bronze/books")
#df_filter = df.filter(f.col("_corrupt_record").isNotNull())

df.printSchema()
df.show(truncate=False)
print("Total Rows: " + str(df.count()))
