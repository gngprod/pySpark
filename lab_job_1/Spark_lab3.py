# Lab3
# Пример обработки полей при помощи select/withColumns
# Вывести название всех устройств,
# если цена больее 50000 вычесть 10% от стоимости назвать поле new_price,
# добавить поле type, используя функцию getTypeDevice
# Итоговое множество содержит поля: product.name, new_price, type
# 1. Создать неявный объект SparkSession, используя метод builder()
# 2. Установить master в local[*]
# 3. Установить имя приложения spark-sql-labs
# 6. Загрузить таблицу product в DataFrame
# 7. В import udf добавить так же when и col из того же пакета
# 8. В методе select() выбрать поле name, используя функцию when().otherwise()
# расчитать новую стоимость девайса

# 9. Используя метод withColumn и функцию getTypeDevice, добавить к выборке поле type
# 10. Вывести результат используя метод show() или записать DataFrame в файл

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("spark-sql-lab3") \
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

product_df = spark.table('product')\
                  .filter(f.col('price') < '50000')\
                  .select(f.col('name'),
                          (f.col('price')*0.9).alias('new_price'))
print('product'), product_df.show()

# -------------------------------------------------------------------
#@f.udf("long")
def getTypeDevice(value: str):
    device_name = value.split(" ")[1]
    if device_name == 'iPhone':
        return 'phone'
    elif device_name == 'iPad':
        return 'tablet'
    elif device_name == 'AirPods':
        return 'headphones'
    elif device_name == 'EarPods':
        return 'headphones'
    elif device_name == 'HomePod':
        return 'speakers'
    else:
        return 'other'


convert = f.udf(lambda z: getTypeDevice(z))
df2 = product_df.select(f.col('name'), f.col('new_price'), convert(f.col("name")).alias('type'))
df2.show()
# https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/

df2.toPandas().to_csv("file:///C:\\Users\\ngzirishvili\\PycharmProjects\\pythonProject2\\out_3")

# https://ask-dev.ru/info/504486/save-content-of-spark-dataframe-as-a-single-csv-file

