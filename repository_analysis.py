from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.appName("repository_analysis").getOrCreate()

schema = StructType([
    StructField("linguagem", StringType(), True),
    StructField("total_repos", IntegerType(), True),
    StructField("ano", IntegerType(), True)
])

df_javascript = spark.read.csv("javascript.csv", header=True, schema=schema)
df_python = spark.read.csv("python.csv", header=True, schema=schema)
df_php = spark.read.csv("php.csv", header=True, schema=schema)
df_java = spark.read.csv("java.csv", header=True, schema=schema)
df_csharp = spark.read.csv("csharp.csv", header=True, schema=schema)

df_all = df_javascript.union(df_python).union(df_php).union(df_java).union(df_csharp)

grouped_df = df_all.groupBy('ano', 'linguagem').agg(F.sum('total_repos').alias('total_repos'))

sorted_df = grouped_df.orderBy('ano')

languages = sorted_df.select('linguagem').distinct().rdd.flatMap(lambda x: x).collect()
distinct_years = sorted_df.select('ano').distinct().rdd.flatMap(lambda x: x).collect()

distinct_years.sort()

values = np.zeros((len(distinct_years), len(languages)))

for i, linguagem in enumerate(languages):
    language_data = sorted_df.filter(F.col('linguagem') == linguagem).orderBy('ano').collect()
    for j, ano in enumerate(distinct_years):
        for data in language_data:
            if data['ano'] == ano:
                values[j][i] = data['total_repos']
                break

fig, ax = plt.subplots(figsize=(10, 6))
bar_space = 0.1

positions = np.arange(len(distinct_years)) + (bar_space * len(languages) / 2)
bar_width = (1 - bar_space) / len(languages)

colors = ['blue', 'yellow', 'purple', 'green', 'red']

for i, linguagem in enumerate(languages):
    ax.bar(positions + (bar_width * i), values[:, i], bar_width, label=linguagem, color=colors[i])

ax.set_xticks(positions)
ax.set_xticklabels(distinct_years)

plt.xlabel('Anos')
plt.ylabel('Total Repositórios')

plt.ticklabel_format(style='plain', axis='y')

plt.legend()

plt.show()