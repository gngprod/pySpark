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

# Laptop(code, model, speed, ram, hd, price)------------------------------
Laptop_schema = StructType([
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
    .schema(Laptop_schema) \
    .load('C:\data_sql\learn2\Laptop\Laptop.txt') \
    .createTempView("Laptop")

Laptop_df = spark.table('Laptop')
print('Laptop_df'), Laptop_df.show()

# Printer(code, model, color, type, price)-------------------------------
Printer_schema = StructType([
                  StructField("code", IntegerType(), nullable=True),
                  StructField("model", StringType(), nullable=True),
                  StructField("color", StringType(), nullable=True),
                  StructField("type", StringType(), nullable=True),
                  StructField("price", IntegerType(), nullable=True)
                  ])
spark.read \
    .format("csv") \
    .option("nullValue", "NULL") \
    .option("delimiter", "\\t") \
    .options(delimiter=',') \
    .schema(Printer_schema) \
    .load('C:\data_sql\learn2\Printer\Printer.txt') \
    .createTempView("Printer")

Printer_df = spark.table('Printer')
print('Printer_df'), Printer_df.show()




