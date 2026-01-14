
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *
from sqlalchemy.sql.sqltypes import NULLTYPE

spark = SparkSession.builder.appName("data_ingestion").master("local[*]").getOrCreate()


#reading all the raw data
raw_ride_requested= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/events/ride_events/ride.json").alias("rr")
raw_user= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/users/user.json")
raw_driver= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/drivers/driver.json")
raw_vehicles= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/db/vehicles/vehicles.json")
raw_transactions= spark.read.format("json").option("multiline", True).load("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Raw/payments/transactions/transaction.json")



#cleaning related to the rides data
rides= raw_ride_requested.withColumn("event_type", F.upper("event_type"))

# due to some network issue in kafka it was not able to acknowledge the event and retries and due to which
#event evt_12345 is getting duplicated so we need to deduplicate this event

rides_window = Window.partitionBy("event_id").orderBy("timestamp")
rides= rides.withColumn("rnk", F.row_number().over(rides_window)).filter(F.col("rnk") == 1).drop("rnk")
clean_rides= rides.groupby("ride_id").agg(
    min(when(F.col("event_type")=="REQUESTED" , F.col("timestamp"))).alias("start_time"),
    max(when(F.col("event_type")=="COMPLETED", F.col("timestamp"))).alias("end_time"),
    max(F.col("user_id")).alias("user_id"),
    max(F.col("driver_id")).alias("driver_id"),
    max(F.col("fare")).alias("fare")

)

# check if end_time is null means the ride is incomplete
clean_rides = clean_rides.withColumn("status", when (F.col("end_time").isNull(), "INCOMPLETE") .otherwise("COMPLETED"))
clean_rides.show()
clean_rides.write.format("parquet").mode("overwrite").save("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_rides")

#cleaning related to the User data
user_window = Window.partitionBy("user_id").orderBy(F.col("updated_at").desc())

clean_user = (
    raw_user
    .withColumn("rn", F.row_number().over(user_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select("user_id", "name", "phone", "email" , "rating" , "created_at")

)
clean_user.show()
clean_user.write.format("parquet").mode("overwrite").save("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_user")


#Driver Deduplication and standardize status
driver_window = Window.partitionBy("driver_id").orderBy(F.col("updated_at").desc())

clean_driver = (
    raw_driver
    .withColumn("rn", F.row_number().over(driver_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select("driver_id", "name", "phone",F.upper(F.col("status")), "rating" , "vehicle_id", "created_at")

)
clean_driver.show()
clean_driver.write.format("parquet").mode("overwrite").save("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_driver")

#clean Vehicle
clean_vehicle = (
    raw_vehicles
    .drop_duplicates(["vehicle_id"])
    .withColumn("vehicle_type", F.upper(F.col("vehicle_type")))
    .select("vehicle_id", "vehicle_type", "plate_number", "capacity")
)
clean_vehicle.show()
clean_vehicle.write.format("parquet").mode("overwrite").save("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_vehicle")
