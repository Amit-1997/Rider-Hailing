
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *
from sqlalchemy.sql.sqltypes import NULLTYPE

spark = SparkSession.builder.appName("data_ingestion").master("local[*]").getOrCreate()


#reading all the raw data
raw_ride_requested= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/events/ride_events/ride_requested.json").alias("rr")
raw_user= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/users/user.json")
raw_driver= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/users/driver.json")
raw_vehicles= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/users/vehicles.json")
raw_transactions= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/payments/transactions/transaction.json")



rides= raw_ride_requested.withColumn("event_type", F.upper("event_type"))

# due to some network issue in kafka it was not able to acknowledge the event and retries and due to which
#event evt_12345 is getting duplicated so we need to deduplicate this event

window = Window.partitionBy("event_id").orderBy("timestamp")
rides= rides.withColumn("rnk", F.row_number().over(window)).filter(F.col("rnk") == 1).drop("rnk")
rides_agg= rides.groupby("ride_id").agg(
    min(when(F.col("event_type")=="REQUESTED" , F.col("timestamp"))).alias("start_time"),
    max(when(F.col("event_type")=="COMPLETED", F.col("timestamp"))).alias("end_time"),
    max(F.col("user_id")).alias("user_id"),
    max(F.col("driver_id")).alias("driver_id"),
    max(F.col("fare")).alias("fare")
)

# check if end_time is null means the ride is incomplete
rides_agg = rides_agg.withColumn("status", when (F.col("end_time").isNull(), "INCOMPLETE") .otherwise("COMPLETED"))







