from pyspark.sql import functions as F
from Code_Repo.common.spark_session import get_spark

from Code_Repo.configs.paths import SILVER_PATH, GOLD_PATH

spark = get_spark("GOLD_rides_SCD1")

silver_rides = spark.read.parquet(f"{SILVER_PATH}/clean_rides")
dim_users = spark.read.parquet(f"{GOLD_PATH}/dim_users")
dim_driver = spark.read.parquet(f"{GOLD_PATH}/dim_drivers")
dim_vehicle = spark.read.parquet(f"{GOLD_PATH}/dim_vehicles")

fact_rides = (
    silver_rides
    .join(dim_users, "user_id")
    .join(dim_driver, "driver_id")
    .join(dim_vehicle, "vehicle_id")
    .withColumn("ride_sk", F.monotonically_increasing_id())
    .withColumn(
        "ride_duration_in_min",
        (F.unix_timestamp("end_time") - F.unix_timestamp("start_time"))/60
)
.withColumn("pickup_date", F.to_date("start_time"))
.select(
    "ride_sk",
    "ride_id",
    "user_sk",
    "driver_sk",
    "vehicle_sk",
    F.col("start_time").alias("pickup_time"),
    F.col("end_time").alias("drop_time"),
    "ride_duration_in_min",
    "fare",
    F.col("status").alias("ride_status"),
    "pickup_date"
)
)
fact_rides.show()
# silver_rides.show()
# dim_users.show()
# dim_driver.show()
# dim_vehicle.show()

fact_rides.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_rides")




