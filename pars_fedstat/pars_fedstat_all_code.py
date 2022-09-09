import os
import pandas as pd
import urllib.request
from pyspark.sql import SparkSession, functions as f


def d_csv_pars_spark(out_dir, name):
    csv_file = os.path.join(out_dir, f'{name}.csv')
    parquet_file = os.path.join(out_dir, f'{name}.parquet')

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()
    spark.read \
        .format("csv") \
        .option("nullValue", "NULL") \
        .options(delimiter=',') \
        .load(csv_file) \
        .createTempView("df")
    df = spark.table('df')
    # df.show()
    df.write.parquet(parquet_file)


def d_csv_par_panda(out_dir, name):
    csv_file = os.path.join(out_dir, f'{name}.csv')
    parquet_file = os.path.join(out_dir, f'{name}.parquet')
    df = pd.read_csv(csv_file, lineterminator='\n', low_memory=False, encoding="ISO-8859-1")
    df.to_parquet(parquet_file)


def d_download_csv(url, out_dir):
    link = f'{url}?format=excel'
    file_name = url.split('fedstat.ru/')[1].replace('/', '_')
    csv_name = os.path.join(out_dir, f'{file_name}.csv')
    # print(link)
    # print(csv_name)
    urllib.request.urlretrieve(link, csv_name)
    d_csv_pars_spark(out_dir, file_name)


if __name__ == '__main__':
    out_dir = 'C:/Users/ngzirishvili/PycharmProjects/pythonProject7/csv'

    url1 = 'https://www.fedstat.ru/indicator/59448'
    d_download_csv(url1, out_dir)
    url2 = 'https://www.fedstat.ru/indicator/42928'
    d_download_csv(url2, out_dir)
    url3 = 'https://www.fedstat.ru/indicator/31452'
    d_download_csv(url3, out_dir)
