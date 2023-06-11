from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

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

df_2023 = sorted_df.filter(F.col('ano') == 2023)

total_repos = df_2023.agg(F.sum('qtd_repos')).collect()[0][0]
percentages = [data['qtd_repos'] / total_repos * 100 for data in df_2023.collect()]

labels = df_2023.select('linguagem').rdd.flatMap(lambda x: x).collect()
sizes = percentages
colors = ['yellow', 'green', 'purple', 'orange', 'cyan', 'blue', 'red']

fig, ax = plt.subplots(figsize=(8, 8))
ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)

ax.set_title('Distribuição de Repositórios em 2023')

plt.show()
