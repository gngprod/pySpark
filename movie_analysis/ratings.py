from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def d_ratings_df(spark):
    # ratings(movie_id: Int, user_id: Int, rate: Int)
    ratings_schema = StructType([
        StructField("movie_id", IntegerType(), nullable=True),
        StructField("user_id", IntegerType(), nullable=True),
        StructField("rate", IntegerType(), nullable=True),
        StructField("string_column", StringType(), nullable=True)
    ])
    spark.read \
        .format("csv") \
        .option("nullValue", "NULL") \
        .options(delimiter=',') \
        .schema(ratings_schema) \
        .load('C:\data_sql\learn3\movielens\\ratings\\ratings.csv') \
        .createTempView("ratings")
    ratings_df = spark.table('ratings')
    return ratings_df

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()
    d_ratings_df(spark).show()
