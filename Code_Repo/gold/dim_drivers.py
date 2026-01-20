from pyspark.sql import functions as F
from Code_Repo.common.spark_session import get_spark
from Code_Repo.common.utils import table_exist
from Code_Repo.configs.paths import SILVER_PATH, GOLD_PATH

spark = get_spark("GOLD_driver_SCD1")

silver_driver = spark.read.parquet(f"{SILVER_PATH}/clean_drivers")

gold_path = f"{GOLD_PATH}/dim_drivers"


if not table_exist(gold_path):

    (
        silver_driver
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )

else:

    user_existing = spark.read.parquet(gold_path)

    updated_drivers = (
        user_existing.alias("t")
        .join(silver_driver.alias("s"), "driver_id", "full_outer")
        .select(
            F.coalesce("s.driver_id", "t.driver_id").alias("driver_id"),
            F.coalesce("s.name", "t.name").alias("name"),
            F.coalesce("s.phone", "t.phone").alias("phone"),
            F.coalesce("s.rating", "t.rating").alias("rating"),
            F.coalesce("s.vehicle_id", "t.vehicle_id").alias("vehicle_id"),
            F.coalesce("s.created_at", "t.created_at").alias("created_at")
        )
    )
    updated_drivers= updated_drivers.withColumn("driver_sk", F.monotonically_increasing_id())
    updated_drivers.show()
    (
        updated_drivers
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )
