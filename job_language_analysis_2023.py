from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.appName("repositoryAnalysis").getOrCreate()

schema = StructType([
    StructField("linguagem", StringType(), True),
    StructField("qtd_vagas", IntegerType(), True)
])

df = spark.read.csv("files/language_jobs.csv", header=True, schema=schema)

grouped_df = df.groupBy('linguagem').agg(F.sum('qtd_vagas').alias('total_vagas'))

sorted_df = grouped_df.orderBy('linguagem')

languages = sorted_df.select('linguagem').rdd.flatMap(lambda x: x).collect()
total_vagas = sorted_df.select('total_vagas').rdd.flatMap(lambda x: x).collect()

fig, ax = plt.subplots(figsize=(10, 6))
bar_width = 0.4

positions = np.arange(len(languages))

ax.bar(positions, total_vagas, bar_width, color='yellow')

ax.set_xticks(positions)
ax.set_xticklabels(languages)

plt.xlabel('Linguagens')
plt.ylabel('Total de Vagas')

plt.ticklabel_format(style='plain', axis='y')

plt.show()
