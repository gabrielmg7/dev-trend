from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import pandas as pd

spark = SparkSession.builder.appName("createJavascriptCsv").getOrCreate()

print('2020...')
responseJavascript2020 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:javascript created:2020-01-01..2020-12-31',
    'per_page': 1
})
dataJavascript2020 = responseJavascript2020.json()

print('2021...')
responseJavascript2021 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:javascript created:2021-01-01..2021-12-31',
    'per_page': 1
})
dataJavascript2021 = responseJavascript2021.json()

print('2022...')
responseJavascript2022 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:javascript created:2022-01-01..2022-12-31',
    'per_page': 1
})
dataJavascript2022 = responseJavascript2022.json()

print('2023...')
responseJavascript2023 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:javascript created:2023-01-01..2023-12-31',
    'per_page': 1
})
dataJavascript2023 = responseJavascript2023.json()

print('Requisições concluídas.')

dfpd = pd.DataFrame({
  'linguagem': ["JavaScript", "JavaScript", "JavaScript", "JavaScript"],
  'total_repos': [dataJavascript2020["total_count"], dataJavascript2021["total_count"], dataJavascript2022["total_count"], dataJavascript2023["total_count"]],
  'ano': [2020, 2021, 2022, 2023]
})

df = spark.createDataFrame(dfpd)

df.write.csv('javascript.csv', header=True, mode="overwrite")

print('Arquivo criado com sucesso.')