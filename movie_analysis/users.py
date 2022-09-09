from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession


def d_users_df():
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()
    # users(user_id: Int,gender: String)
    users_schema = StructType([
                      StructField("user_id", IntegerType(), nullable=True),
                      StructField("gender", StringType(), nullable=True)
                      ])
    spark.read \
        .format("csv") \
        .option("nullValue", "NULL") \
        .options(delimiter=',') \
        .schema(users_schema) \
        .load('C:\data_sql\learn3\movielens\\users\\users.csv') \
        .createTempView("users")
    users_df = spark.table('users')
    return users_df.show()


if __name__ == '__main__':
    d_users_df()
