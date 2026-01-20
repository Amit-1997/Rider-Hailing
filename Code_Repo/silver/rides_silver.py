from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import RAW_PATH,SILVER_PATH
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *


def main():
    try:
        # job_name = input("Pipeline name for Rides: ")
        spark= get_spark("Silver_Rides_Transform")
        raw_ride_requested= spark.read.format("json").option("multiline", True).load(f"{RAW_PATH}/events/ride_events/ride.json").alias("rr")
        #cleaning related to the rides data




        rides= raw_ride_requested.withColumn("event_type", F.upper("event_type"))

        # due to some network issue in kafka it was not able to acknowledge the event and retries and due to which
        #event evt_12345 is getting duplicated so we need to deduplicate this event

        rides_window = Window.partitionBy("event_id").orderBy("timestamp")
        rides= rides.withColumn("rnk", F.row_number().over(rides_window)).filter(F.col("rnk") == 1).drop("rnk")
        clean_rides= rides.groupby("ride_id").agg(
            min(when(F.col("event_type")=="REQUESTED" , F.col("timestamp"))).alias("start_time").cast("timestamp"),
            max(when(F.col("event_type")=="COMPLETED", F.col("timestamp"))).alias("end_time").cast("timestamp"),
            max(F.col("user_id")).alias("user_id"),
            max(F.col("vehicle_id")).alias("vehicle_id"),
            max(F.col("driver_id")).alias("driver_id"),
            max(F.col("fare")).alias("fare")

        )

        # check if end_time is null means the ride is incomplete
        clean_rides = clean_rides.withColumn("status", when (F.col("end_time").isNull(), "INCOMPLETE") .otherwise("COMPLETED"))
        clean_rides.show()
        clean_rides.write.format("parquet").mode("overwrite").save(f"{SILVER_PATH}/clean_rides")

    except:
        print("Pipeline name is incorrect, Please check and provide the correct pipeline name.")


    spark.stop()

if __name__ == "__main__":
    main()