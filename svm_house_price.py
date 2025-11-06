from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("HousePriceSVM").getOrCreate()

# Sample house data: area, bedrooms, location, price_group
data = spark.createDataFrame([
    (1200, 3, 1, "High"),
    (800, 2, 2, "Medium"),
    (600, 2, 1, "Low"),
    (1500, 4, 1, "High"),
    (700, 2, 2, "Low"),
    (1300, 3, 2, "High")
], ["area","bedrooms","location","price_group"])

# Binary target: High=1, Others=0
data = data.withColumn("label", (data.price_group == "High").cast("double"))

# Features
assembler = VectorAssembler(inputCols=["area","bedrooms","location"], outputCol="features")
data = assembler.transform(data)

# SVM before normalization
svm = LinearSVC(featuresCol="features", labelCol="label")
model = svm.fit(data)
pred = model.transform(data)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
acc_before = evaluator.evaluate(pred)

# Feature normalization
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
scaler_model = scaler.fit(data)
data = scaler_model.transform(data)

# SVM after normalization
svm2 = LinearSVC(featuresCol="scaledFeatures", labelCol="label")
model2 = svm2.fit(data)
pred2 = model2.transform(data)
acc_after = evaluator.evaluate(pred2)

print(f"Accuracy before normalization: {acc_before:.3f}")
print(f"Accuracy after normalization: {acc_after:.3f}")

spark.stop()
