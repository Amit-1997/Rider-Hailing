
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("data_ingestion").master("local[*]").getOrCreate()


df= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/events/ride_events/ride_requested.json")
df.show()
