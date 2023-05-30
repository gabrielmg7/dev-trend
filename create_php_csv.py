from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import pandas as pd

spark = SparkSession.builder.appName("createPhpCsv").getOrCreate()

print('2020...')
responsePhp2020 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:php created:2020-01-01..2020-12-31',
    'per_page': 1
})
dataPhp2020 = responsePhp2020.json()

print('2021...')
responsePhp2021 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:php created:2021-01-01..2021-12-31',
    'per_page': 1
})
dataPhp2021 = responsePhp2021.json()

print('2022...')
responsePhp2022 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:php created:2022-01-01..2022-12-31',
    'per_page': 1
})
dataPhp2022 = responsePhp2022.json()

print('2023...')
responsePhp2023 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:php created:2023-01-01..2023-12-31',
    'per_page': 1
})
dataPhp2023 = responsePhp2023.json()

print('Requisições concluídas.')

dfpd = pd.DataFrame({
  'linguagem': ["Php", "Php", "Php", "Php"],
  'total_repos': [dataPhp2020["total_count"], dataPhp2021["total_count"], dataPhp2022["total_count"], dataPhp2023["total_count"]],
  'ano': [2020, 2021, 2022, 2023]
})

df = spark.createDataFrame(dfpd)

df.write.csv('php.csv', header=True, mode="overwrite")

print('Arquivo criado com sucesso.')