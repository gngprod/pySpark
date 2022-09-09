# Lab10
# Пример использования join, groupBy, agg
# Необходимо расчитать для каждого клиента, стоимость общей закупки каждого товара,
# максимальный объем заказанного товара, минимальную стоимость заказа,
# среднюю стоимость заказа за первую половину 2018 года, заказ должен быть доставлен
# Итоговое множество содержит поля: customer.name, product.name, sum(order.number_of_product * price),
# max(order.number_of_product), min(order.number_of_product * price), avg(order.number_of_product * price)
# 1. Создать неявный объект SparkSession, используя метод builder()
# 2. Установить master в local[*]
# 3. Установить имя приложения spark-sql-labs
# 6. Загрузить в DataFrame таблицу order
# 7. Выбрать транзакции со статусом delivered и датой заказа с 2018-01-01 по 2018-06-30
# 8. Загрузить в DataFrame таблицу customer, выбрать поля:
# id далее customer_id, email
# 9. Загрузить в DataFrame таблицу product, выбрать поля:
# id далее product_id, name далее product_name
# 10. Выполнить перекрестное соединение DataFrame из п.8 и п.9
# 11. Выполнить внутреннее соединение DataFrame из п.10 и п.7
# 12. Выполнить группировку по полям customer_name, product_name
# 13. Расчитать сумму по стоимости заказа, максимальный объем заказ, минимальную сумму заказа, среднюю сумму заказа.
# 14. Вывести результат используя метод show() или записать DataFrame в файл

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("spark-sql-lab5") \
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
# print('order'), order_df.show()

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

customer_df = spark.table('customer').select(f.col('id').alias('customer_id'), f.col('name').alias('customer_name'))
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

product_df = spark.table('product').select(f.col('id').alias('product_id'), f.col('name').alias('product_name'), f.col('price'))
# print('product'), product_df.show()

# -------------------------------------------------------------------
print('1)')
new_order_df = order_df.filter(f.col('status') == 'delivered')\
        .filter(f.col('orderDate') > '2018-01-01')\
        .filter(f.col('orderDate') < '2018-06-30')
print('new_order_df'), new_order_df.show()

print('2)')
join_customer_product = customer_df.join(product_df, customer_df.customer_id == product_df.product_id, "inner")\
                                   .select(f.col('customer_id'), f.col('customer_name'), f.col('product_name'), f.col('price'))
print('join_customer_product'), join_customer_product.show()

print('3)')
join_customer_product_order = join_customer_product.join(new_order_df, join_customer_product.customer_id == new_order_df.customerID, 'inner')\
                        .select(f.col('customer_name'), f.col('product_name'), f.col('orderDate'), f.col('numberOfProducts'), f.col('price'))
print('join_customer_product_order'), join_customer_product_order.show()

print('4)')
out_df = join_customer_product_order.groupBy('customer_name').agg(f.sum('price').alias('sum_price'),
                                                                  f.max('numberOfProducts').alias('max_number'),
                                                                  f.min('price').alias('min_price'),
                                                                  f.avg('price').alias('avg_price'))
print('out_df'), out_df.show()

out_df.toPandas().to_csv("C:/Users/ngzirishvili/PycharmProjects/pythonProject2/out5.csv")

