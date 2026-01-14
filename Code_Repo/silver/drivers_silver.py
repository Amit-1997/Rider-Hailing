
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import get_path
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *

job_name = input("Pipeline name for Drivers: ")
path = get_path(job_name)
spark= get_spark("Silver_Driver_Transform")
raw_drivers= spark.read.format("json").option("multiline", True).load(path)
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
clean_drivers.write.format("parquet").mode("overwrite").save("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_drivers")
