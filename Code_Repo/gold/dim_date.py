from pyspark.sql import functions as F
from Code_Repo.common.spark_session import get_spark
from Code_Repo.configs.paths import GOLD_PATH



def main():
    spark= get_spark("GOLD_Date_Transform")


    dates_df = (
        spark.range(0, 3650)
        .select(
            F.date_add(
                F.to_date(F.lit("2020-01-01")),
                F.col("id").cast("int")
            ).alias("date")
        )
        .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("month", F.month("date"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
    )

    dates_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_date")

    spark.stop()

if __name__ == "__main__":
    main()
