
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import get_path
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import *

try:
    job_name = input("Pipeline name for Users: ")
    path = get_path(job_name)
    spark= get_spark("Silver_Users_Transform")
    raw_users= spark.read.format("json").option("multiline", True).load(path)

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
    clean_user.write.format("parquet").mode("overwrite").save("/Users/amitchaurasia/PycharmProjects/Rider-Hailing/Silver/clean_users")
except:
    print("Pipeline name is incorrect, Please check and provide the correct pipeline name.")