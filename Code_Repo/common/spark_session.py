
from pyspark.sql import SparkSession

def get_spark(app_name="RiderHailingApp"):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


