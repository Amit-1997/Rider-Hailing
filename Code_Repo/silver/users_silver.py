
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import RAW_PATH,SILVER_PATH
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *

try:
    job_name = input("Pipeline name for Users: ")
    spark= get_spark("Silver_Users_Transform")
    raw_users= spark.read.format("json").option("multiline", True).load(f"{RAW_PATH}/db/users/user.json")

    #cleaning related to the User data
    user_window = Window.partitionBy("user_id").orderBy(F.col("updated_at").desc())

    clean_user = (
        raw_users
        .withColumn("rn", F.row_number().over(user_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select("user_id", "name", "phone", "email" , "rating" , "created_at")

    )
    clean_user.show()
    clean_user.write.format("parquet").mode("overwrite").save(f"{SILVER_PATH}/clean_users")
except:
    print("Pipeline name is incorrect, Please check and provide the correct pipeline name.")