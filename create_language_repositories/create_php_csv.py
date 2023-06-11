from pyspark.sql import SparkSession
import requests
import pandas as pd

linguagem = 'php'

spark = SparkSession.builder.appName("create_php_csv").getOrCreate()

print('2008...')
response2008 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2008-01-01..2008-12-31',
    'per_page': 1
})
data2008 = response2008.json()

print('2010...')
response2010 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2010-01-01..2010-12-31',
    'per_page': 1
})
data2010 = response2010.json()

print('2012...')
response2012 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2012-01-01..2012-12-31',
    'per_page': 1
})
data2012 = response2012.json()

print('2014...')
response2014 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2014-01-01..2014-12-31',
    'per_page': 1
})
data2014 = response2014.json()

print('2016...')
response2016 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2016-01-01..2016-12-31',
    'per_page': 1
})
data2016 = response2016.json()

print('2018...')
response2018 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2018-01-01..2018-12-31',
    'per_page': 1
})
data2018 = response2018.json()

print('2020...')
response2020 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2020-01-01..2020-12-31',
    'per_page': 1
})
data2020 = response2020.json()

print('2022...')
response2022 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2022-01-01..2022-12-31',
    'per_page': 1
})
data2022 = response2022.json()

print('2023...')
response2023 = requests.get("https://api.github.com/search/repositories", params = {
    'q': f'language:{linguagem} created:2023-01-01..2023-12-31',
    'per_page': 1
})
data2023 = response2023.json()

print('Requisições concluídas.')

dfpd = pd.DataFrame({
  'linguagem': [linguagem, linguagem, linguagem, linguagem, linguagem, linguagem, linguagem, linguagem, linguagem],
  'qtd_repos': [data2008["total_count"], data2010["total_count"], data2012["total_count"], data2014["total_count"], data2016["total_count"], data2018["total_count"], data2020["total_count"], data2022["total_count"], data2023["total_count"]],
  'ano': [2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2023]
})

df = spark.createDataFrame(dfpd)

df_pandas = df.toPandas()
df_pandas.to_csv('../files/php.csv', index=False)

print('Arquivo criado com sucesso.')