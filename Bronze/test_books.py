from pyspark.sql import SparkSession, functions as f

spark = SparkSession.builder.appName("Book Test").getOrCreate()

df = spark.read.option("inferSchema","true").parquet("Data/Bronze/books")
#df_filter = df.filter(f.col("_corrupt_record").isNotNull())

df.printSchema()
df.show(truncate=False)
print("Total Rows: " + str(df.count()))
