from pyspark.sql.utils import AnalysisException
from Code_Repo.common.spark_session import get_spark

def table_exist(path):

    spark = get_spark("utils")
    try:
        spark.read.parquet(path)
        return True
    except AnalysisException:
        return False