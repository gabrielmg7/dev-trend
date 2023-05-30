from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import pandas as pd

spark = SparkSession.builder.appName("createCSharpCsv").getOrCreate()

print('2020...')
responseCSharp2020 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:c# created:2020-01-01..2020-12-31',
    'per_page': 1
})
dataCSharp2020 = responseCSharp2020.json()

print('2021...')
responseCSharp2021 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:c# created:2021-01-01..2021-12-31',
    'per_page': 1
})
dataCSharp2021 = responseCSharp2021.json()

print('2022...')
responseCSharp2022 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:c# created:2022-01-01..2022-12-31',
    'per_page': 1
})
dataCSharp2022 = responseCSharp2022.json()

print('2023...')
responseCSharp2023 = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'language:c# created:2023-01-01..2023-12-31',
    'per_page': 1
})
dataCSharp2023 = responseCSharp2023.json()

print('Requisições concluídas.')

dfpd = pd.DataFrame({
  'linguagem': ["C#", "C#", "C#", "C#"],
  'total_repos': [dataCSharp2020["total_count"], dataCSharp2021["total_count"], dataCSharp2022["total_count"], dataCSharp2023["total_count"]],
  'ano': [2020, 2021, 2022, 2023]
})

df = spark.createDataFrame(dfpd)

df.write.csv('csharp.csv', header=True, mode="overwrite")

print('Arquivo criado com sucesso.')