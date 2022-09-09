# Lab1
# Загрузка таблицы, Создать объект SparkSession.
# 1. Создать неявный объект SparkSession, используя метод builder()
# 2. Установить master в local[*]
# 3.Установить имя приложения spark - sql - labs
# 4.Задать следующие параметы для DataFrameReader
# .format("csv")
# .option("nullValue", "NULL")
# .option("delimiter", "\t")
# .schema(схема_таблицы)
# .load(путь)
# .createTempView("customer")
# 5.Выполнить селект используя SQL и вывести результат

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("spark-sql-lab1") \
    .getOrCreate()

customer_schema = StructType([
                  StructField("id", IntegerType(), nullable=True),
                  StructField("name", StringType(), nullable=True),
                  StructField("email", StringType(), nullable=True),
                  StructField("joinDate", DateType(), nullable=True),
                  StructField("status", StringType(), nullable=True),
                  ])

spark.read \
    .format("csv") \
    .option("nullValue", "NULL") \
    .option("delimiter", "\\t") \
    .schema(customer_schema) \
    .load('C:\data_sql\customer\customer.csv') \
    .createTempView("customer")

df = spark.table('customer')
# df = spark.table('customer').filter(f.col('email') == 'rob.ert@gmail.com') # конструкция WHERE

df.show()

#               SELECT
# spark.sql('SELECT * FROM customer').show()  # прямым SQL запросом
# df.select('id', 'name').show()              # запросом через функии Python
# df.select(f.col('id'),f.col('name')).show() # запросом через функции Python

# https://spark.apache.org/docs/latest/sql-getting-started.html#scalar-functions