from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when

spark = SparkSession.builder.appName("LoanAmountDistribution").getOrCreate()

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

spark.stop()