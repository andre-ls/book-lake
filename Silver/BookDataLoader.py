import requests
import json
from pyspark.sql import SparkSession

class BookDataLoader:

    def __init__(self, url, schema):
        self.url = url
        self.schema = schema
        self.spark = SparkSession.builder.appName("Book Data Loader").getOrCreate()
        self.sparkContext = self.spark.sparkContext

    def requestData(self):
        return requests.get(self.url).json()

    def convertToDataframe(self, data):
        return self.spark.read.schema(self.schema).json(self.sparkContext.parallelize([json.dumps(data)]))

    def getData(self):
        json = self.requestData()
        return self.convertToDataframe(json)
