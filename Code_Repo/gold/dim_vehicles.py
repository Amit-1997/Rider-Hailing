from pyspark.sql import functions as F
from Code_Repo.common.spark_session import get_spark
from Code_Repo.common.utils import table_exist
from Code_Repo.configs.paths import SILVER_PATH, GOLD_PATH


def main():
    spark = get_spark("GOLD_vehicle_SCD1")
    silver_vehicles = spark.read.parquet(f"{SILVER_PATH}/clean_vehicles")
    gold_path = f"{GOLD_PATH}/dim_vehicles"


    if not table_exist(gold_path):
        (
            silver_vehicles
            .write
            .mode("overwrite")
            .parquet(gold_path)
        )

    else:

        user_existing = spark.read.parquet(gold_path)

        updated_vehicles = (
            user_existing.alias("t")
            .join(silver_vehicles.alias("s"), "vehicle_id", "full_outer")
            .select(
                F.coalesce("s.vehicle_id", "t.vehicle_id").alias("vehicle_id"),
                F.coalesce("s.vehicle_type", "t.vehicle_type").alias("vehicle_type"),
                F.coalesce("s.plate_number", "t.plate_number").alias("plate_number"),
                F.coalesce("s.capacity", "t.capacity").alias("capacity"),
            )
        )
        updated_vehicles= updated_vehicles.withColumn("vehicle_sk", F.monotonically_increasing_id())
        updated_vehicles.show()
        (
            updated_vehicles
            .write
            .mode("overwrite")
            .parquet(gold_path)
        )
    spark.stop()

if __name__ == "__main__":
    main()
