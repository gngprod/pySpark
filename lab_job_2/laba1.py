# Задание 1
# Для каждого значения скорости ПК, превышающего 600 МГц, определите среднюю цену ПК с такой же скоростью.
# Вывести: speed, средняя цена.
#
# SELECT pc.speed, AVG(pc.price)
# FROM pc
# WHERE pc.speed > 600
# GROUP BY pc.speed

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from pyspark.sql import SparkSession, functions as f

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("sql-lab1") \
    .getOrCreate()

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

#------------------------------------------------------------------------
print('1)')
df1 = Pc_df.groupBy('speed').avg('price').filter(f.col('speed') > 600).orderBy('speed')
print('df1'), df1.show()

# SELECT pc.speed, AVG(pc.price)
# FROM pc
# WHERE pc.speed > 600
# GROUP BY pc.speed


