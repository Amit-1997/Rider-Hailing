
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import RAW_PATH,SILVER_PATH
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *

def main():
    try:
        job_name = input("Pipeline name for Users: ")
        spark= get_spark("Silver_Vehicles_Transform")
        raw_vehicles= spark.read.format("json").option("multiline", True).load(f"{RAW_PATH}/db/vehicles/vehicles.json")

        #clean Vehicle
        clean_vehicle = (
            raw_vehicles
            .drop_duplicates(["vehicle_id"])
            .withColumn("vehicle_type", F.upper(F.col("vehicle_type")))
            .select("vehicle_id", "vehicle_type", "plate_number", "capacity")
        )
        clean_vehicle.show()
        clean_vehicle.write.format("parquet").mode("overwrite").save(f"{SILVER_PATH}/clean_vehicles")
    except:
        print("Pipeline name is incorrect, Please check and provide the correct pipeline name.")


    spark.stop()

if __name__ == "__main__":
    main()