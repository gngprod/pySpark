# Задание 2
# Вывести все строки из таблицы Product, кроме трех строк с наименьшими номерами моделей и трех строк с наибольшими номерами моделей.
#   Select maker, model, type from
#   (
#   Select
#   row_number() over (order by model) p1,
#   row_number() over (order by model DESC) p2,
#   from Product
#   ) t1
#   where p1 > 3 and p2 > 3

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f
from pyspark.sql import Window

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("sql-lab1") \
    .getOrCreate()

# Product(maker, model, type)______________________________________________
Product_schema = StructType([
                  StructField("maker", StringType(), nullable=True),
                  StructField("model", StringType(), nullable=True),
                  StructField("type", StringType(), nullable=True)
                  ])
spark.read \
    .format("csv") \
    .option("nullValue", "NULL") \
    .option("delimiter", "\\t") \
    .options(delimiter=',') \
    .schema(Product_schema) \
    .load('C:\data_sql\learn2\product\product.txt') \
    .createTempView("Product")

Product_df = spark.table('Product')
print('Product_df'), Product_df.show()

#------------------------------------------------------------------------
print('1)')

w1 = Window().partitionBy().orderBy('model')
w2 = Window().partitionBy().orderBy(f.desc('model'))

df1 = Product_df.select(f.col('maker'), f.col('model'), f.col('type'),
                        f.row_number().over(w1).alias('p1'),
                        f.row_number().over(w2).alias('p2'))\
                .filter(f.col('p1') > 3)\
                .filter(f.col('p2') > 3)
print('df1'), df1.show()

#   Select maker, model, type from
#   (
#   Select
#   row_number() over (order by model) p1,
#   row_number() over (order by model DESC) p2,
#   from Product
#   ) t1
#   where p1 > 3 and p2 > 3