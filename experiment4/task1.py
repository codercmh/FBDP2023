from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when

spark = SparkSession.builder.appName("task1").getOrCreate()

file_path = "/user/cmh/input/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
#df.show(n=4, truncate=10)

bin_edges = [i * 10000 for i in range(0, int(df.agg({"AMT_CREDIT": "max"}).collect()[0][0] // 10000) + 2)]

df = df.withColumn("credit_bin", col("AMT_CREDIT"))

for edge, next_edge in zip(bin_edges[:-1], bin_edges[1:]):
    df = df.withColumn("credit_bin", when((col("AMT_CREDIT") >= edge) & (col("AMT_CREDIT") < next_edge), f"{edge}-{next_edge}").otherwise(col("credit_bin")))

result = df.groupBy("credit_bin").count().orderBy("credit_bin")

result_formatted = result.select(
    expr("struct(int(split(credit_bin, '-')[0]), int(split(credit_bin, '-')[1])) as bin_range"),
    col("count").alias("record_count")
)

result_formatted.show(truncate=False)



df = df.withColumn("dif", col("AMT_CREDIT") - col("AMT_INCOME_TOTAL"))

df_sorted = df.orderBy(col("dif").desc())

top_10_high_diff = df_sorted.limit(10)

df_sorted_asc = df.orderBy(col("dif").asc())

top_10_low_diff = df_sorted_asc.limit(10)

top_10_high_diff.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "dif").show()
top_10_low_diff.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "dif").show()

#result_formatted.write.csv("/user/cmh/output/credit_distribution_result.csv", header=True)

spark.stop()