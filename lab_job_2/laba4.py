# Задание 4
# Найдите производителей принтеров,
# которые производят ПК с наименьшим объемом RAM и с самым быстрым процессором среди всех ПК,
# имеющих наименьший объем RAM. Вывести: Maker
#   SELECT DISTINCT maker                                                       5)
#   FROM product
#   WHERE model IN (
#                   SELECT model                                                3)
#                   FROM pc
#                   WHERE ram = (
#                                SELECT MIN(ram)                                1)
#                                FROM pc
#                               )
#                   AND speed = (
#                                SELECT MAX(speed)                              2)
#                                FROM pc
#                                WHERE ram = (
#                                             SELECT MIN(ram)                   1)
#                                             FROM pc
#                                            )
#                               )
#                  )
#   AND maker IN (
#                 SELECT maker                                                  4)
#                 FROM product
#                 WHERE type='printer'
#                )

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f, DataFrame
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

# PC(code, model, speed, ram, hd, cd, price)________________________________
pc_schema = StructType([
                  StructField("code", IntegerType(), nullable=True),
                  StructField("model", StringType(), nullable=True),
                  StructField("speed", IntegerType(), nullable=True),
                  StructField("ram", IntegerType(), nullable=True),
                  StructField("hd", IntegerType(), nullable=True),
                  StructField("cd", StringType(), nullable=True),
                  StructField("price", IntegerType(), nullable=True)
                  ])
spark.read \
    .format("csv") \
    .option("nullValue", "NULL") \
    .option("delimiter", "\\t") \
    .options(delimiter=',') \
    .schema(pc_schema) \
    .load('C:\data_sql\learn2\pc\pc.txt') \
    .createTempView("PC")

Pc_df = spark.table('PC')
print('Pc_df'), Pc_df.show()

#  ------------------------------------------------------------------------
print('1)')
# SELECT MIN(ram)                  1)
# FROM pc
df1 = Pc_df.select(f.min('ram').alias('min_ram'))
print('df1'), df1.show()
df1_x = df1.collect()[0][0]

print('2)')
# SELECT MAX(speed)                              2)
# FROM pc
# WHERE ram = (df1)
df2 = Pc_df.filter(f.col('ram') == df1_x).select(f.max('speed'))
print('df2'), df2.show()
df2_x = df2.collect()[0][0]

print('3)')
# SELECT model                                                3)
# FROM pc
# WHERE ram = (df1)
# AND speed = (df2)
df3 = Pc_df.select(f.col('model')).filter(f.col('ram') == df1_x).filter(f.col('speed') == df2_x)
print('df3'), df3.show()
df3_x = []
for i in range(len(df3.collect())):
    df3_x.append(df3.collect()[i][0])
print(df3_x)

print('4)')
# SELECT maker                                                4)
# FROM product
# WHERE type='printer'
df4 = Product_df.select(f.col('maker')).filter(f.col('type') == 'Printer')
print('df4'), df4.show()
df4_x = []
for i in range(len(df4.collect())):
    df4_x.append(df4.collect()[i][0])
print(df4_x)

print('5)')
#   SELECT DISTINCT maker                                     5)
#   FROM product
#   WHERE model IN (df3)
#   AND maker IN (df4)
df5 = Product_df.filter(f.col('model').isin(df3_x))\
                .filter(f.col('maker').isin(df4_x)).select(f.col('maker')).distinct()
print('df5'), df5.show()


