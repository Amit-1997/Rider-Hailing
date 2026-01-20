from pyspark.sql import functions as F
from Code_Repo.common.spark_session import get_spark
from Code_Repo.common.utils import table_exist
from Code_Repo.configs.paths import SILVER_PATH, GOLD_PATH
from Code_Repo.gold.fact_rides import fact_rides

spark = get_spark("GOLD_payments")

silver_payments = spark.read.parquet(f"{SILVER_PATH}/clean_payments")
fact_rides = spark.read.parquet(f"{GOLD_PATH}/fact_rides")

fact_payments = (
    silver_payments
    .join(fact_rides.select("ride_id", "ride_sk"), "ride_id")
    .withColumn("payment_sk", F.monotonically_increasing_id())
    .withColumn("paid_date", F.to_date("paid_at"))
    .select(
        "payment_sk",
        "ride_sk",
        "payment_mode",
        "payment_status",
        "paid_date"
    )
)

fact_payments.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_payments")


