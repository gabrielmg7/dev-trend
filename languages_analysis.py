from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.appName("repository_analysis").getOrCreate()

schema_repos = StructType([
    StructField("linguagem", StringType(), True),
    StructField("qtd_repos", IntegerType(), True),
    StructField("ano", IntegerType(), True)
])

schema_jobs = StructType([
    StructField("linguagem", StringType(), True),
    StructField("qtd_vagas", IntegerType(), True)
])

schema_salary = StructType([
    StructField("linguagem", StringType(), True),
    StructField("media_salario", IntegerType(), True)
])

df_javascript = spark.read.csv("files/javascript.csv", header=True, schema=schema_repos)
df_python = spark.read.csv("files/python.csv", header=True, schema=schema_repos)
df_php = spark.read.csv("files/php.csv", header=True, schema=schema_repos)
df_java = spark.read.csv("files/java.csv", header=True, schema=schema_repos)
df_csharp = spark.read.csv("files/csharp.csv", header=True, schema=schema_repos)
df_cplusplus = spark.read.csv("files/cplusplus.csv", header=True, schema=schema_repos)
df_ruby = spark.read.csv("files/ruby.csv", header=True, schema=schema_repos)

df_jobs = spark.read.csv("files/language_jobs.csv", header=True, schema=schema_jobs)
df_salary = spark.read.csv("files/language_salary_average.csv", header=True, schema=schema_salary)

df_all = df_javascript.union(df_python).union(df_php).union(df_java).union(df_csharp).union(df_cplusplus).union(df_ruby)
grouped_df = df_all.filter(F.col('ano') == 2023).groupBy('linguagem').agg(F.sum('qtd_repos').alias('qtd_repos'))

df_grouped = grouped_df.join(df_jobs, on='linguagem').join(df_salary, on='linguagem')

labels = df_grouped.select('linguagem').rdd.flatMap(lambda x: x).collect()
qtd_vagas = df_grouped.select('qtd_vagas').rdd.flatMap(lambda x: x).collect()
media_salario = df_grouped.select('media_salario').rdd.flatMap(lambda x: x).collect()
qtd_repos = df_grouped.select('qtd_repos').rdd.flatMap(lambda x: x).collect()

fig, ax = plt.subplots(figsize=(10, 6))
bar_width = 0.4
positions = np.arange(len(labels))

ax.bar(positions - bar_width/2, qtd_vagas, bar_width, color='green', label='Qtd Vagas')
ax.bar(positions + bar_width/2, media_salario, bar_width, color='blue', label='Média de Salário')

ax.set_xticks(positions)
ax.set_xticklabels(labels)
plt.xlabel('Linguagem')
plt.ylabel('Quantidade')
plt.title('Quantidade de Vagas e Média de Salário por Linguagem em 2023')

ax2 = ax.twinx()
ax2.plot(positions, qtd_repos, color='red', linestyle='--', marker='o', label='Qtd Repos (Escala Diferente)')
ax2.ticklabel_format(style='plain', axis='y')
ax2.set_ylabel('Quantidade de Repositórios')

lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax.legend(lines + lines2, labels + labels2, loc='upper right')

plt.show()
