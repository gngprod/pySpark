# Lab2
# Пример считывания таблиц
# Создать объект SparkSession
# Загрузить таблицу используя метод table/sql
# 1. Создать неявный объект SparkSession, используя метод builder()
# 2. Установить master в local[*]
# 3. Установить имя приложения spark-sql-labs
# 6. Загрузить таблицу customer, используя метод table()
# 7. Загрузить таблицу product, используя метод sql() и в качестве аргумента запрос: "FROM product"
# 8. Загрузить таблицу order, используя метод sql и в качестве аргумента запрос:
# "SELECT DISTINCT customer_id, order_date FROM order WHERE status = 'delivered'"
# 9. Вывести таблицы используя метод show()
# в качестве аргумента можно указать лимит в кол-во строк и тип выводимой строки полный/урезанный

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("spark-sql-lab2") \
    .getOrCreate()


# CUSTOMER-------------------------------------------------------------
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

customer_df = spark.table('customer')
print('customer'), customer_df.show()


# PRODUCT-------------------------------------------------------------
product_schema = StructType([
                  StructField("id", IntegerType(), nullable=True),
                  StructField("name", StringType(), nullable=True),
                  StructField("price", DoubleType(), nullable=True),
                  StructField("numberOfProducts", IntegerType(), nullable=True)
                  ])
spark.read \
    .format("csv") \
    .option("nullValue", "NULL") \
    .option("delimiter", "\\t") \
    .schema(product_schema) \
    .load('C:\data_sql\product\product.csv') \
    .createTempView("product")

product_df = spark.sql('SELECT * FROM product')
print('product'), product_df.show()


# ORDER-------------------------------------------------------------
order_schema = StructType([
                  StructField("customerID", IntegerType(), nullable=True),
                  StructField("orderID",IntegerType(), nullable=True),
                  StructField("productID", IntegerType(), nullable=True),
                  StructField("numberOfProducts", IntegerType(), nullable=True),
                  StructField("orderDate", DateType(), nullable=True),
                  StructField("status", StringType(), nullable=True)
                  ])
spark.read \
    .format("csv") \
    .option("nullValue", "NULL") \
    .option("delimiter", "\\t") \
    .schema(order_schema) \
    .load('C:\data_sql\order\order.csv') \
    .createTempView("order")

order_df = spark.sql('SELECT  DISTINCT  customerID, orderDate FROM order WHERE status = "delivered"')
print('order'), order_df.show()

