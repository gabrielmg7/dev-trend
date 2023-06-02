from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.appName("repositoryAnalysis").getOrCreate()

schema = StructType([
    StructField("tech", StringType(), True),
    StructField("qtd_repos", IntegerType(), True)
])

df = spark.read.csv("files/tech_repos_2023.csv", header=True, schema=schema)

grouped_df = df.groupBy('tech').agg(F.sum('qtd_repos').alias('total_repos'))

sorted_df = grouped_df.orderBy('tech')

technologies = sorted_df.select('tech').rdd.flatMap(lambda x: x).collect()
total_repos = sorted_df.select('total_repos').rdd.flatMap(lambda x: x).collect()

fig, ax = plt.subplots(figsize=(10, 6))
bar_width = 0.4

positions = np.arange(len(technologies))

ax.bar(positions, total_repos, bar_width, color='blue')

ax.set_xticks(positions)
ax.set_xticklabels(technologies)

plt.xlabel('Tecnologias')
plt.ylabel('Total de Repositórios')

plt.ticklabel_format(style='plain', axis='y')

plt.show()
