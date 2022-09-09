# Lab6
# Пример использования оконных выражений
# Необходимо определить самый популярный продукт у клиента
# Итоговое множество содержит поля: customer.name, product.name
# 1. Создать неявный объект SparkSession, используя метод builder()
# 2. Установить master в local[*]
# 3. Установить имя приложения spark-sql-labs
# 6. Загрузить таблицу product в DataFrame
# 7. Выбрать поля: id далее product_id, name далее product_name
# 8. Загрузить таблицу customer в DataFrame
# 9. Выбрать поля: id далее customer_id, name далее customer_name
# 10. Выполнить перекрестное соединение DataFrame из п.7 и п.9
# 11. Загрузить таблицу order в DataFrame
# 12. Выполнить группировку по полю customer_id, product_id
# 13. Расчитать сумму по полю number_of_product, далее sum_num_of_product
# 14. Добавить import org.apache.spark.sql.expressions.Window
# 15. Написать оконную функцию с партицированием по полю customer_id
# и сортирвокой в порядке убывания по полю sum_num_of_product
# 16. Использую аналитичексую функцию row_number и оконное выражение из п.15
# добавить поле rn
# 17. Выбрать только те строки, в которых значение поля rn = 1
# 18. Выполнить внутреннее соединение DataFrame из п.17 и п.10 по полям:
# customer_id, product_id
# 19. Выбрать поля customer_name, product_name
# 20. Вывести результат используя метод show() или записать DataFrame в файл

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f
from pyspark.sql import Window


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("spark-sql-lab5") \
    .getOrCreate()

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

# -------------------------------------------------------------------
print('1)')
join_customer_product = customer_df.join(product_df, customer_df.customer_id == product_df.product_id, "inner")\
                                   .select(f.col('customer_id'), f.col('customer_name'), f.col('product_name'))
print('join_customer_product'), join_customer_product.show()

print('2)')
new_order_df = order_df.groupBy('customerID').sum('numberOfProducts').alias('sum_num_of_product')
print('new_order_df'), new_order_df.show()

print('3)')
w1 = Window().partitionBy('customerID').orderBy('numberOfProducts')
w2 = Window().partitionBy('customerID').orderBy('numberOfProducts')
w_df = order_df.select(f.col('customerID'), f.col('numberOfProducts'),
                        f.sum('numberOfProducts').over(w1).alias('sum_num_of_product'),
                        f.row_number().over(w1).alias('rn'))\
                .filter(f.col('rn') == '1')
print('w_df'), w_df.show()

print('4)')
out_df = join_customer_product.join(w_df, join_customer_product.customer_id == w_df.customerID, 'inner')\
    .select(f.col('customer_name'), f.col('product_name'))
print('out_df'), out_df.show()

out_df.toPandas().to_csv("C:/Users/ngzirishvili/PycharmProjects/pythonProject2/out6.csv")






