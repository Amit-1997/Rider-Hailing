
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import get_path
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *


try:
    job_name = input("Pipeline name for Users: ")
    path = get_path(job_name)
    spark= get_spark("Silver_Vehicles_Transform")
    raw_vehicles= spark.read.format("json").option("multiline", True).load(path)

    #clean Vehicle
    clean_vehicle = (
        raw_vehicles
        .drop_duplicates(["vehicle_id"])
        .withColumn("vehicle_type", F.upper(F.col("vehicle_type")))
        .select("vehicle_id", "vehicle_type", "plate_number", "capacity")
    )
    clean_vehicle.show()
    clean_vehicle.write.format("parquet").mode("overwrite").save("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_vehicles")
except:
    print("Pipeline name is incorrect, Please check and provide the correct pipeline name.")
