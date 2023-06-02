from pyspark.sql import SparkSession
import requests
import pandas as pd

spark = SparkSession.builder.appName("create_tech_2023_csv").getOrCreate()

headers = {
    'Authorization': 'Bearer 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'  # substitua "your_token_here" pelo seu token
}

print('react...')
responseReact = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'react.js OR reactJs created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataReact = responseReact.json()

print('angular...')
responseAngular = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'angular OR angular.js created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataAngular = responseAngular.json()

print('vue...')
responseVue = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'vue OR vue.js created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataVue = responseVue.json()

print('laravel...')
responseLaravel = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'laravel created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataLaravel = responseLaravel.json()

print('flask...')
responseFlask = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'flask created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataFlask = responseFlask.json()

print('django...')
responseDjango = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'django created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataDjango = responseDjango.json()

print('flutter...')
responseFlutter = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'flutter created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataFlutter = responseFlutter.json()

print('react-native...')
responseReactNative = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'react-native OR reactNative created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataReactNative = responseReactNative.json()

print('ruby-on-rails...')
responseRubyOnRails = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'rails OR ruby-on-rails created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataRubyOnRails = responseRubyOnRails.json()

print('node...')
responseNode = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'node OR node.js created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataNode = responseNode.json()

print('asp.net...')
responseAspNet = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'asp.net OR aspNet created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataAspNet = responseAspNet.json()

print('.net-core...')
responseNetCore = requests.get("https://api.github.com/search/repositories", params = {
    'q': '.net-core OR netCore created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataNetCore = responseNetCore.json()

print('spring...')
responseSpring = requests.get("https://api.github.com/search/repositories", params = {
    'q': 'spring OR spring-boot OR springBoot created:2023-01-01..2023-12-31',
    'per_page': 1
},headers=headers)
dataSpring = responseSpring.json()

print('Requisições concluídas.')

dfpd = pd.DataFrame({
  'tech': ['react', 'angular', 'vue.js', 'laravel', 'flask', 'django', 'flutter', 'react-native', 'ruby-on-rails', 'node', 'asp.net', '.net-core', 'spring'],
  'qtd_repos': [dataReact["total_count"], dataAngular["total_count"], dataVue["total_count"], dataLaravel["total_count"], dataFlask["total_count"], dataDjango["total_count"], dataFlutter["total_count"], dataReactNative["total_count"], dataRubyOnRails["total_count"], dataNode["total_count"], dataAspNet["total_count"], dataNetCore["total_count"], dataSpring["total_count"]]
})

df = spark.createDataFrame(dfpd)

df_pandas = df.toPandas()
df_pandas.to_csv('../files/tech_repos_2023.csv', index=False)

print('Arquivo criado com sucesso.')
