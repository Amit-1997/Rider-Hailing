
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import RAW_PATH,SILVER_PATH
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *

try:
    job_name = input("Pipeline name for Payments: ")

    spark= get_spark("Silver_Payment_Transform")
    raw_transactions= spark.read.format("json").option("multiline", True).load(f"{RAW_PATH}/payments/transactions/transaction.json")

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
    clean_payment.write.format("parquet").mode("overwrite").save(f"{SILVER_PATH}/clean_payments")
except:
    print("Pipeline name is incorrect, Please check and provide the correct pipeline name.")