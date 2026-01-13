from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *
from sqlalchemy.sql.sqltypes import NULLTYPE

spark = SparkSession.builder.appName("data_ingestion").master("local[*]").getOrCreate()

spark.read.format("parquet").load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_rides").show()