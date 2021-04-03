# !/usr/bin/python
# -*- coding: utf-8 -*-

###########################################################################
# Descrição: Script de leitura, tratamento e gravação de dados
# Autor: Cristina Cruz
# Data: 03/04/2021
############################################################################
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

def read_json():
    df = spark.read.json('C:\Temp\yelp_academic_dataset_review\yelp_academic_dataset_review.json')
    df2 = df.select('business_id', 'stars')
    return df2

def calcula_media(df):
    df2 = df.groupBy('business_id').agg(F.avg('stars').cast('Decimal(10,4)').alias('media'))
    return df2

def create_table():
    qry = """
    CREATE TABLE IF NOT EXISTS `db`.`tb_media_stars` (
    business_id string,
    stars decimal(10,4)
    )
    USING PARQUET
    OPTIONS (
    path '/datalake/db/media_stars'
    )    
    """
    spark.sql(qry)

if __name__ == "__main__":
    df_dados = read_json()
    df_media = calcula_media(df_dados)

    create_table()

    df_media.write.mode("overwrite").format("parquet").save("/datalake/db/media_stars")