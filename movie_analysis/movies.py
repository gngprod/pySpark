from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def d_movie_df(spark):
    # movies(movie_id: Int,movie_name: String,genre: String)
    movies_schema = StructType([
        StructField("movie_id", IntegerType(), nullable=True),
        StructField("movie_name", StringType(), nullable=True),
        StructField("genre", StringType(), nullable=True),
        StructField("string_column", StringType(), nullable=True)
    ])
    spark.read \
        .format("csv") \
        .option("nullValue", "NULL") \
        .options(delimiter=',') \
        .schema(movies_schema) \
        .load('C:\data_sql\learn3\movielens\movies\movies.csv') \
        .createTempView("movies")
    movies_df = spark.table('movies')
    return movies_df


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()

    d_movie_df(spark).show()
