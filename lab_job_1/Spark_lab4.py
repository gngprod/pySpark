# Lab4
# Пример использования filter, join, crossJoin, orderBy
# Вывести информацию о клиенте email, название продукта
# и кол-во доставленного товара за первую половину 2018 года
# Итоговое множество содержит поля: customer.email, product.name, order.order_date, order.number_of_product
# 1. Создать неявный объект SparkSession, используя метод builder()
# 2. Установить master в local[*]
# 3. Установить имя приложения spark-sql-labs
# 6. Загрузить в DataFrame таблицу order
# 7. Загрузить в DataFrame таблицу customer, выбрать поля:
# id далее customer_id, email
# 8. Загрузить в DataFrame таблицу product, выбрать поля:
# id далее product_id, name далее product_name
# 9. Выполнить перекрестное соединение DataFrame из п.7 и п.8
# 11. Выполнить внутреннее соединение по полю customer_id, product_id
# 10. Выбрать транзакции co статусом "delivered" и датой заказа с 2018-01-01 по 2018-06-30
# 12. Выбрать поля email, product_name, order_date, number_of_products
# 13. Выполнить сортировку по полю email, order_date
# 14. Вывести результат используя метод show() или записать DataFrame в файл

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("spark-sql-lab4") \
    .getOrCreate()

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

order_df = spark.table('order')
print('order'), order_df.show()

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

customer_df = spark.table('customer').select(f.col('id').alias('customer_id'), f.col('email'))
# print('customer'), customer_df.show()

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

product_df = spark.table('product').select(f.col('id').alias('product_id'), f.col('name').alias('product_name'))
# print('product'), product_df.show()

# -------------------------------------------------------------------
print('1)')
join_customer_product = customer_df.join(product_df, customer_df.customer_id == product_df.product_id, "inner")\
                                   .select(f.col('customer_id'), f.col('email'), f.col('product_name'))
print('join_customer_product'), join_customer_product.show()

print('2)')
new_order_df = order_df.filter(f.col('status') == 'delivered')\
        .filter(f.col('orderDate') > '2018-01-01')\
        .filter(f.col('orderDate') < '2018-06-30')
# rowsBetween()
print('new_order_df'), new_order_df.show()

print('3)')
join_customer_product_order = join_customer_product.join(new_order_df, join_customer_product.customer_id == new_order_df.customerID, 'inner')\
                                                   .select(f.col('email'), f.col('product_name'), f.col('orderDate'), f.col('numberOfProducts'))
print('join_customer_product_order'), join_customer_product_order.show()

print('4)')
new_join_customer_product_order = join_customer_product_order.sort(f.col('email').asc(), f.col('orderDate').asc())
print('new_join_customer_product_order'), new_join_customer_product_order.show()

new_join_customer_product_order.toPandas().to_csv("C:/Users/ngzirishvili/PycharmProjects/pythonProject2/out4.csv")
