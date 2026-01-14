
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import get_path
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *


job_name = input("Pipeline name for Payments: ")
path = get_path(job_name)
spark= get_spark("Silver_Payment_Transform")
raw_transactions= spark.read.format("json").option("multiline", True).load(path)

#clean payment
payment_window = Window.partitionBy("ride_id").orderBy(F.col("paid_at").desc())

clean_payment = (
    raw_transactions
    .withColumn("paid_at", F.to_timestamp("paid_at"))
    .withColumn("rn", F.row_number().over(payment_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select("payment_id", "ride_id", "amount", "payment_mode" , "payment_status", "paid_at")

)
clean_payment.show()
clean_payment.write.format("parquet").mode("overwrite").save("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_payment")
