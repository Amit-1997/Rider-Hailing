from pyspark.sql import functions as F
from Code_Repo.common.spark_session import get_spark
from Code_Repo.common.utils import table_exist
from Code_Repo.configs.paths import SILVER_PATH, GOLD_PATH

spark = get_spark("GOLD_User_SCD1")

silver_user = spark.read.parquet(f"{SILVER_PATH}/clean_users")

gold_path = f"{GOLD_PATH}/dim_users"


if not table_exist(gold_path):

    (
        silver_user
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )

else:

    user_existing = spark.read.parquet(gold_path)

    updated_users = (
        user_existing.alias("t")
        .join(silver_user.alias("s"), "user_id", "full_outer")
        .select(
            F.coalesce("s.user_id", "t.user_id").alias("user_id"),
            F.coalesce("s.name", "t.name").alias("name"),
            F.coalesce("s.email", "t.email").alias("email"),
            F.coalesce("s.phone", "t.phone").alias("phone"),
            F.coalesce("s.rating", "t.rating").alias("rating"),
            F.coalesce("s.created_at", "t.created_at").alias("created_at")
        )
    )
    updated_users= updated_users.withColumn("user_sk", F.monotonically_increasing_id())
    updated_users.show()
    (
        updated_users
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )
