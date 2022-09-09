# Задание 3
#   Найти тех производителей ПК, все модели ПК которых имеются в таблице PC.
#   SELECT p.maker
#   FROM product p
#   LEFT JOIN pc ON pc.model = p.model
#   WHERE p.type = 'PC'
#   GROUP BY p.maker
#   HAVING COUNT(p.model) = COUNT(pc.model)

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

#------------------------------------------------------------------------
print('1)')
join_product_pc_df = Product_df.join(Pc_df, Pc_df.model == Product_df.model, 'Inner')               # JOIN
print('join_product_pc_df'), join_product_pc_df.show()

print('2)')
df1 = join_product_pc_df.select(f.col('maker'), f.col('product.model').alias('p_model'),            # WHERE
                                f.col('PC.model').alias('pc_model'))\
                        .filter(f.col('type') == 'PC')
print('df1'), df1.show()

print('3)')
df2 = df1.groupBy('maker').agg(f.count('p_model').alias('sum_p_model'),                             # HAVING
                               f.count('pc_model').alias('sum_pc_model'))\
                                .where(f.col('sum_pc_model') == f.col('sum_p_model'))
print('df2'), df2.show()

#   SELECT p.maker
#   FROM product p
#   LEFT JOIN pc ON pc.model = p.model
#   WHERE p.type = 'PC'
#   GROUP BY p.maker
#   HAVING COUNT(p.model) = COUNT(pc.model)