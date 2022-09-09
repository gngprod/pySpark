import csv
import json
import os
import shutil
import zipfile
from math import ceil

from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.types import *


class GeoFedstatUtils:
    @staticmethod
    def rename_files(full_local_path: str):
        file_format = full_local_path.split('.')[-1]
        part_count = 0
        for filename in os.listdir(full_local_path):
            if filename.endswith(file_format):
                part_count += 1
        for filename in os.listdir(full_local_path):
            if filename.endswith(file_format) and part_count == 1:
                full_filename = os.path.join(full_local_path, filename)
                os.rename(full_filename, full_local_path + 'tmp')
                shutil.rmtree(full_local_path)
                os.rename(full_local_path + 'tmp', full_local_path)
                return
            if filename.endswith(file_format) and part_count > 1:
                full_filename = os.path.join(full_local_path, filename)
                part_num = int(filename[5:10])
                os.rename(full_filename, full_local_path + '_' + str(part_num).zfill(5))
        if part_count > 1:
            shutil.rmtree(full_local_path)

    @staticmethod
    def check_and_delete(file_name):
        if os.path.exists(file_name) and os.path.isfile(file_name):
            os.remove(file_name)
            print(f'Path {file_name} exists. Delete it.')
        elif os.path.exists(file_name) and not os.path.isfile(file_name):
            print(f'Path {file_name} exists. Delete it.')
            shutil.rmtree(file_name)

    @staticmethod
    def spark_init() -> SparkSession:
        os.environ['JAVA_OPTS'] = "-Xms5g -Xmx5g"
        spark: SparkSession = SparkSession.builder.appName('converter') \
            .config("spark.sql.caseSensitive", "true").master('local[*]').getOrCreate()
        # spark.sparkContext.setLogLevel('INFO')
        return spark

    @staticmethod
    def get_df(spark: SparkSession, full_local_path: str, file_format='parquet', options=None) -> DataFrame:
        reader = spark.read
        if options is not None:
            for option in options:
                reader = reader.option(option[0], option[1])
        return reader.load('file:///' + full_local_path, format=file_format)

    @staticmethod
    def save_df(df: DataFrame, full_local_path, file_format='parquet', part_num=1, rewrite=True, rename=False,
                mode='overwrite'):
        file_name = f'{full_local_path}.{file_format}'
        spark_file_name = f'file:///{full_local_path}.{file_format}'
        if rewrite:
            GeoFedstatUtils.check_and_delete(file_name)

        df.repartition(part_num).write.save(spark_file_name, format=file_format, mode=mode)
        if rename:
            GeoFedstatUtils.rename_files(file_name)

    @staticmethod
    def get_cols_list(schema: StructType) -> list:
        res_list = []
        for field in schema:
            if isinstance(field.dataType, StructType):
                res_list += [".".join([field.name, str(col)]) for col in get_cols_list(field.dataType)]
            else:
                res_list.append(field.name)
        return res_list

    @staticmethod
    def check_column_exists(col_name: str, df):

        def get_cols_list(schema: StructType) -> list:
            res_list = []
            for field in schema:
                if isinstance(field.dataType, StructType):
                    res_list += [".".join([field.name, str(col)]) for col in get_cols_list(field.dataType)]
                else:
                    res_list.append(field.name)
            return res_list

        schema = df.schema
        cols_list = get_cols_list(schema)
        if col_name in cols_list:
            return f.col(col_name)

        print(f'Column {col_name} does not exist. Fill null')
        return f.lit(None).cast('string')

    @staticmethod
    def write_in_file(directory: str, filename: str, data, open_format='w', makedirs=True, json_flag=False,
                      encoding='UTF-8'):
        if makedirs:
            os.makedirs(directory, exist_ok=True)
        full_path = os.path.join(directory, filename)
        if 'b' in open_format:
            f = open(full_path, open_format)
        else:
            f = open(full_path, open_format, encoding=encoding)

        if json_flag:
            json.dump(data, f)
        else:
            f.write(data)
        f.close()

    @staticmethod
    def cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
        for field in schema:
            if isinstance(field, StructField):
                if isinstance(field.dataType, (StringType, TimestampType, DateType)):
                    df = df.withColumn(field.name,
                                       GeoFedstatUtils.check_column_exists(field.name, df).cast(field.dataType))
                elif isinstance(field.dataType, DecimalType):
                    df = df.withColumn(field.name, f.regexp_replace(GeoFedstatUtils.check_column_exists(field.name, df),
                                                                    r"[\s|\xa0]+", ""))
                    df = df.withColumn(field.name,
                                       f.regexp_replace(GeoFedstatUtils.check_column_exists(field.name, df), r",",
                                                        ".").cast(field.dataType))
                else:
                    df = df.withColumn(field.name,
                                       f.regexp_replace(GeoFedstatUtils.check_column_exists(field.name, df),
                                                        r"[\s|\xa0]+", "").cast(field.dataType))
            else:
                raise ValueError("Incorrect Schema")
        return df

    @staticmethod
    def transpose_by(df: DataFrame, by, col_name, value):
        cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
        kvs = f.explode(f.array([
            f.struct(f.lit(c).alias(col_name), f.col(c).alias(value)) for c in cols
        ])).alias("kvs")
        return df.select(by + [kvs]).select(by + [f'kvs.{col_name}', f'kvs.{value}'])
