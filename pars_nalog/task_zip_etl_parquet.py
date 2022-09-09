from pyspark.sql import SparkSession, functions as f
import os
import shutil


def d_zip_etl_parquet(out_dir):
    spark = SparkSession \
        .builder \
        .config('spark.sql.caseSensitive', True) \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()
    df = spark.read.format('xml') \
        .options(rowTag='Документ') \
        .load(f'{out_dir}*.xml')
    # df.printSchema()
# _____________________________________________________________________________________1
    df_msp_subjects = df.select(
                    f.col('_ИдДок').alias('Id_Doc'),
                    f.col('_ДатаСост').alias('Status_Date'),
                    f.col('_ДатаВклМСП').alias('Date_MSP_in'),
                    f.col('_ВидСубМСП').alias('MSP_Subject'),
                    f.col('_ПризНовМСП').alias('MSP_New'),
                    f.col('_КатСубМСП').alias('MSP_Category'),
                    f.col('_СведСоцПред').alias('MSP_Social'),
                    f.col('_ССЧР').alias('SSCHR'),
                    f.col('СведМН._КодРегион').alias('Region_Code'),
                    f.col('СведМН.Регион._Тип').alias('Region_Type'),
                    f.col('СведМН.Регион._Наим').alias('Region_Name'),
                    f.col('СведМН.Город._Тип').alias('City_Type'),
                    f.col('СведМН.Город._Наим').alias('City_Name'),
                    f.col('СведМН.Район._Тип').alias('District_Type'),
                    f.col('СведМН.Район._Наим').alias('District_Name'),
                    f.col('СведМН.НаселПункт._Тип').alias('Town_Type'),
                    f.col('СведМН.НаселПункт._Наим').alias('Town_Name'),
                    f.col('СвОКВЭД.СвОКВЭДОсн._КодОКВЭД').alias('OKVED_main_Code'),
                    f.col('СвОКВЭД.СвОКВЭДОсн._НаимОКВЭД').alias('OKVED_main_Name'),
                    f.col('СвОКВЭД.СвОКВЭДОсн._ВерсОКВЭД').alias('OKVED_main_Version'),
                    f.current_date().alias("actual_date_from")
                    )
    name_file_msp_subjects = os.path.join(out_dir, 'msp_subjects.parquet')
    df_msp_subjects.write.parquet(name_file_msp_subjects)
    df_msp_subjects.drop()
# _____________________________________________________________________________________2
    df_msp_ip = df.select(
                    f.col('_ИдДок').alias('Id_Doc'),
                    f.col('ИПВклМСП._ИННФЛ').alias('INN_IP'),
                    f.col('ИПВклМСП.ФИОИП._Имя').alias('IP_Name'),
                    f.col('ИПВклМСП.ФИОИП._Фамилия').alias('IP_Surname'),
                    f.col('ИПВклМСП.ФИОИП._Отчество').alias('IP_MiddleName')
                    )
    name_file_msp_ip = os.path.join(out_dir, 'msp_ip.parquet')
    df_msp_ip.write.parquet(name_file_msp_ip)
    df_msp_ip.drop()
# _____________________________________________________________________________________3
    df_msp_ul = df.select(
                    f.col('_ИдДок').alias('Id_Doc'),
                    f.col('ОргВклМСП._НаимОрг').alias('INN_UL'),
                    f.col('ОргВклМСП._НаимОргСокр').alias('Organization_Name'),
                    f.col('ОргВклМСП._ИННЮЛ').alias('Organization_Abbreviated_Name')
                    )
    name_file_msp_ul = os.path.join(out_dir, 'msp_ul.parquet')
    df_msp_ul.write.parquet(name_file_msp_ul)
    df_msp_ul.drop()
# _____________________________________________________________________________________4
    df_msp_production = df.select(
                    f.col('_ИдДок').alias('Id_Doc'),
                    f.col('СвПрод._КодПрод').alias('Production_code'),
                    f.col('СвПрод._НаимПрод').alias('Production_name'),
                    f.col('СвПрод._ПрОтнПрод').alias('Production_type')
                    )
    name_file_msp_production = os.path.join(out_dir, 'msp_production.parquet')
    df_msp_production.write.parquet(name_file_msp_production)
    df_msp_production.drop()
# _____________________________________________________________________________________5
    df_msp_pp = df.select(
                    f.col('_ИдДок').alias('Id_Doc'),
                    f.col('СвПрогПарт._НаимЮЛ_ПП').alias('Organization_Name_PP'),
                    f.col('СвПрогПарт._ИННЮЛ_ПП').alias('INN_UL_PP'),
                    f.col('СвПрогПарт._НомДог').alias('Contract_num'),
                    f.col('СвПрогПарт._ДатаДог').alias('Contract_date'),
                    )
    name_file_msp_pp = os.path.join(out_dir, 'msp_pp.parquet')
    df_msp_pp.write.parquet(name_file_msp_pp)
    df_msp_pp.drop()


def d_task_zip_etl_parquet():
    out_dir = '/home/ubuntuos/nalog/data/out_file/'
    shutil.rmtree(f'{out_dir}msp_subjects.parquet', ignore_errors=True)
    shutil.rmtree(f'{out_dir}msp_ip.parquet', ignore_errors=True)
    shutil.rmtree(f'{out_dir}msp_ul.parquet', ignore_errors=True)
    shutil.rmtree(f'{out_dir}msp_production.parquet', ignore_errors=True)
    shutil.rmtree(f'{out_dir}msp_pp.parquet', ignore_errors=True)
    d_zip_etl_parquet(out_dir)


if __name__ == '__main__':
    out_dir = '/home/ubuntuos/nalog/data/out_file/'
    shutil.rmtree(f'{out_dir}msp_subjects.parquet', ignore_errors=True)
    shutil.rmtree(f'{out_dir}msp_ip.parquet', ignore_errors=True)
    shutil.rmtree(f'{out_dir}msp_ul.parquet', ignore_errors=True)
    shutil.rmtree(f'{out_dir}msp_production.parquet', ignore_errors=True)
    shutil.rmtree(f'{out_dir}msp_pp.parquet', ignore_errors=True)
    d_zip_etl_parquet(out_dir)
