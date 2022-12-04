import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType

load_dotenv()
dataDirectory = os.environ.get('DATA_DIRECTORY')
spark = SparkSession.builder.appName("Load Ratings").getOrCreate()

df = spark.read.option("inferSchema","true").text(dataDirectory + "/History/ol_dump_ratings_2022-10-31.txt")

@f.udf(returnType = StringType())
def cleanRow(row):
    return re.sub("/[{}]|(\/\w*\/)","",row)

def cleanData(df):
    return df.select(cleanRow(f.col('value')).alias("value"))

def splitData(df):
    split_df =  df.withColumn('work_key',f.split(df['value'], '\t').getItem(0))\
                     .withColumn('edition_key',f.split(df['value'], '\t').getItem(1))\
                     .withColumn('rating',f.split(df['value'], '\t').getItem(2))\
                     .withColumn('date',f.split(df['value'], '\t').getItem(3))

    return split_df.drop(split_df.value)

def convertDateColumn(df):
    return df.withColumn("date",f.to_date(f.col("date"),'yyyy-MM-dd'))

df = cleanData(df)
df = splitData(df)
df = convertDateColumn(df)

df.write.option("mode","overwrite").parquet(dataDirectory + "/Bronze/ratings")
