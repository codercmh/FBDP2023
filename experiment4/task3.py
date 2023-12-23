from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("task3").getOrCreate()

train_data = spark.read.csv("/user/cmh/input/train.csv", header=True, inferSchema=True)
test_data = spark.read.csv("/user/cmh/input/test.csv", header=True, inferSchema=True)

train_data = train_data.select([col for col in train_data.columns if col != "SK_ID_CURR"])
test_data = test_data.select([col for col in test_data.columns if col != "SK_ID_CURR"])

combined_data = train_data.union(test_data)

categorical_columns = [col_name for col_name, data_type in combined_data.dtypes if data_type == "string"]
numeric_columns = [col_name for col_name, data_type in combined_data.dtypes if data_type != "string" and col_name != "TARGET"]

print("categorical:",categorical_columns)
print("numeric:",numeric_columns)

indexers = [StringIndexer(inputCol=col_name, outputCol=col_name+"_index", handleInvalid="keep") for col_name in categorical_columns]
encoders = [OneHotEncoder(inputCol=col_name+"_index", outputCol=col_name+"_encoded") for col_name in categorical_columns]

assembler = VectorAssembler(inputCols=numeric_columns + [col_name+"_encoded" for col_name in categorical_columns], outputCol="features")

lr = LogisticRegression(featuresCol="features", labelCol="TARGET", maxIter=10, regParam=0, elasticNetParam=0)
rf = RandomForestClassifier(featuresCol="features", labelCol="TARGET", numTrees=50, maxDepth=10)

pipeline_lr = Pipeline(stages=indexers + encoders + [assembler, lr])
pipeline_rf = Pipeline(stages=indexers + encoders + [assembler, rf])

model_lr = pipeline_lr.fit(train_data)
model_rf = pipeline_rf.fit(train_data)

predictions_lr = model_lr.transform(test_data)
predictions_rf = model_rf.transform(test_data)

evaluator = BinaryClassificationEvaluator(labelCol="TARGET")

accuracy_lr = evaluator.evaluate(predictions_lr)
print("Logistic Regression Model Accuracy:", accuracy_lr)

accuracy_rf = evaluator.evaluate(predictions_rf)
print("Random Forest Model Accuracy:", accuracy_rf)

spark.stop()