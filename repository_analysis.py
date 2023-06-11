from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.appName("repository_analysis").getOrCreate()

schema = StructType([
    StructField("linguagem", StringType(), True),
    StructField("qtd_repos", IntegerType(), True),
    StructField("ano", IntegerType(), True)
])

df_javascript = spark.read.csv("files/javascript.csv", header=True, schema=schema)
df_python = spark.read.csv("files/python.csv", header=True, schema=schema)
df_php = spark.read.csv("files/php.csv", header=True, schema=schema)
df_java = spark.read.csv("files/java.csv", header=True, schema=schema)
df_csharp = spark.read.csv("files/csharp.csv", header=True, schema=schema)
df_cplusplus = spark.read.csv("files/cplusplus.csv", header=True, schema=schema)
df_ruby = spark.read.csv("files/ruby.csv", header=True, schema=schema)

df_all = df_javascript.union(df_python).union(df_php).union(df_java).union(df_csharp).union(df_cplusplus).union(df_ruby)

grouped_df = df_all.groupBy('ano', 'linguagem').agg(F.sum('qtd_repos').alias('qtd_repos'))

sorted_df = grouped_df.orderBy('ano')

languages = sorted_df.select('linguagem').distinct().rdd.flatMap(lambda x: x).collect()
distinct_years = sorted_df.select('ano').distinct().rdd.flatMap(lambda x: x).collect()

distinct_years.sort()

values = np.zeros((len(distinct_years), len(languages)))

distinct_years = [ano for ano in distinct_years if ano <= 2022]

values = np.zeros((len(distinct_years), len(languages)))

for i, linguagem in enumerate(languages):
    language_data = sorted_df.filter(F.col('linguagem') == linguagem).orderBy('ano').collect()
    for j, ano in enumerate(distinct_years):
        for data in language_data:
            if data['ano'] == ano:
                values[j][i] = data['qtd_repos']
                break

fig, ax = plt.subplots(figsize=(10, 6))

positions = np.arange(len(distinct_years))

colors = ['purple', 'red', 'yellow', 'cyan', 'orange', 'blue', 'green']

for i, linguagem in enumerate(languages):
    ax.plot(distinct_years, values[:, i], label=linguagem, color=colors[i])

ax.set_xticks(distinct_years)
ax.set_xticklabels(distinct_years)

plt.xlabel('Anos')
plt.ylabel('Total Repositórios')

plt.ticklabel_format(style='plain', axis='y')

plt.legend()

plt.show()
