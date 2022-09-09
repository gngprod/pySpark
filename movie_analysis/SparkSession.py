from pyspark.sql import SparkSession


def SparkS():
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()
    return spark
