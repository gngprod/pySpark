from pyspark.sql import SparkSession, functions as f
import os
from convert import get_exploded


def d_zip_etl_parquet(out_dir):
    out_dir2 = 'file:///C:\\Users\\ngzirishvili\\PycharmProjects\\pythonProject9\\xml_test/*.xml'
    xml_file = os.path.join(out_dir2, 'VO_RRMSPSV_0000_9965_20220610_000e9756-a9b5-45d9-a2c3-9dd94fe5d022.csv')
    parquet_file = os.path.join(out_dir, f'out.parquet')

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()
    df = spark.read.format('xml') \
        .options(rowTag='Документ') \
        .load(out_dir2)
    # df.show()
    df.printSchema()
    # df.select('ИПВклМСП.*').show()
    # .select(f.explode("ФИОИП"))
    # df2 = get_exploded(df)
    # df2.show()





if __name__ == '__main__':
    out_dir = 'C:/Users/ngzirishvili/PycharmProjects/pythonProject9/data'
    d_zip_etl_parquet(out_dir)
