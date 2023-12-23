from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs

spark = SparkSession.builder.appName("task2").getOrCreate()

file_path = "/user/cmh/input/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

male_customers = df.filter(col("CODE_GENDER") == "M")

children_counts = male_customers.groupBy("CNT_CHILDREN").count()
total_count = male_customers.count()
children_percentage = children_counts.withColumn("percent", col("count") / total_count)

children_percentage_sorted = children_percentage.orderBy(col("percent").desc())

children_percentage_sorted.select("CNT_CHILDREN", "percent").show()


df = df.withColumn("DAYS_BIRTH_ABS", abs(col("DAYS_BIRTH")))
df = df.withColumn("avg_income", col("AMT_INCOME_TOTAL") / col("DAYS_BIRTH_ABS"))

filtered_df = df.filter(col("avg_income") > 1)

sorted_df = filtered_df.orderBy(col("avg_income").desc())

result_df = sorted_df.select("SK_ID_CURR", "avg_income")

result_df.show()

result_df.write.csv("/user/cmh/output/avg_income.csv", header=True, mode="overwrite")


spark.stop()