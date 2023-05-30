from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import pandas as pd

spark = SparkSession.builder.appName("createPythonCsv").getOrCreate()

print('2020...')
responsePython2020 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:python created:2020-01-01..2020-12-31',
    'per_page': 1
})
dataPython2020 = responsePython2020.json()

print('2021...')
responsePython2021 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:python created:2021-01-01..2021-12-31',
    'per_page': 1
})
dataPython2021 = responsePython2021.json()

print('2022...')
responsePython2022 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:python created:2022-01-01..2022-12-31',
    'per_page': 1
})
dataPython2022 = responsePython2022.json()

print('2023...')
responsePython2023 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:python created:2023-01-01..2023-12-31',
    'per_page': 1
})
dataPython2023 = responsePython2023.json()

print('Requisições concluídas.')

dfpd = pd.DataFrame({
  'linguagem': ["Python", "Python", "Python", "Python"],
  'total_repos': [dataPython2020["total_count"], dataPython2021["total_count"], dataPython2022["total_count"], dataPython2023["total_count"]],
  'ano': [2020, 2021, 2022, 2023]
})

df = spark.createDataFrame(dfpd)

df.write.csv('python.csv', header=True, mode="overwrite")

print('Arquivo criado com sucesso.')