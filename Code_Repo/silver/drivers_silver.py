
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import RAW_PATH,SILVER_PATH
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *

try:
    job_name = input("Pipeline name for Drivers: ")

    spark= get_spark("Silver_Driver_Transform")
    raw_drivers= spark.read.format("json").option("multiline", True).load(f"{RAW_PATH}/db/drivers/driver.json")
    #Driver Deduplication and standardize status
    driver_window = Window.partitionBy("driver_id").orderBy(F.col("updated_at").desc())

    clean_drivers = (
        raw_drivers
        .withColumn("rn", F.row_number().over(driver_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select("driver_id", "name", "phone",F.upper(F.col("status")), "rating" , "vehicle_id", "created_at")

    )
    clean_drivers.show()
    clean_drivers.write.format("parquet").mode("overwrite").save(f"{SILVER_PATH}/clean_drivers")
except:
    print("Pipeline name is incorrect, Please check and provide the correct pipeline name.")