from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import pandas as pd

spark = SparkSession.builder.appName("createJavaCsv").getOrCreate()

print('2020...')
responseJava2020 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:java created:2020-01-01..2020-12-31',
    'per_page': 1
})
dataJava2020 = responseJava2020.json()

print('2021...')
responseJava2021 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:java created:2021-01-01..2021-12-31',
    'per_page': 1
})
dataJava2021 = responseJava2021.json()

print('2022...')
responseJava2022 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:java created:2022-01-01..2022-12-31',
    'per_page': 1
})
dataJava2022 = responseJava2022.json()

print('2023...')
responseJava2023 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:java created:2023-01-01..2023-12-31',
    'per_page': 1
})
dataJava2023 = responseJava2023.json()

print('Requisições concluídas.')

dfpd = pd.DataFrame({
  'linguagem': ["Java", "Java", "Java", "Java"],
  'total_repos': [dataJava2020["total_count"], dataJava2021["total_count"], dataJava2022["total_count"], dataJava2023["total_count"]],
  'ano': [2020, 2021, 2022, 2023]
})

df = spark.createDataFrame(dfpd)

df.write.csv('java.csv', header=True, mode="overwrite")

print('Arquivo criado com sucesso.')