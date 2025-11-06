# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, mean
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import matplotlib.pyplot as plt

# === Step 1: Initialize Spark Session ===
spark = SparkSession.builder.appName("HeartDiseasePrediction").getOrCreate()

# === Step 2: Create Your Own Database (heart.csv sample) ===
# In practice, replace this with: data = spark.read.csv("heart.csv", header=True, inferSchema=True)
data = spark.createDataFrame([
    (63, 1, 145, 233, 1, 150, 0, 2.3, 0, 0, 1, 1, "yes"),
    (37, 1, 130, 250, 0, 187, 0, 3.5, 0, 0, 2, 3, "no"),
    (41, 0, 130, 204, 0, 172, 0, 1.4, 2, 0, 1, 3, "no"),
    (56, 1, 120, 236, 0, 178, 0, 0.8, 0, 0, 2, 1, "yes"),
    (57, 0, 120, 354, 0, 163, 1, 0.6, 0, 1, 2, 1, "yes"),
    (57, 1, 140, 192, 0, 148, 0, 0.4, 1, 0, 1, 1, "no"),
    (56, 0, 140, 294, 0, 153, 0, 1.3, 0, 0, 2, 0, "yes"),
    (44, 1, 120, 263, 0, 173, 0, 0.0, 0, 0, 1, 1, "no"),
    (52, 1, 172, 199, 1, 162, 0, 0.5, 0, 0, 2, 3, "yes"),
    (44, 0, 108, 141, 0, 175, 0, 0.6, 0, 0, 1, 1, "no")
], ["age","sex","trestbps","chol","fbs","thalach","exang","oldpeak",
    "slope","ca","thal","cp","target"])

# === Step 3: Basic EDA (Exploratory Data Analysis) ===
print("\n=== Dataset Schema ===")
data.printSchema()

print("\n=== First 5 Rows ===")
data.show(5)

print("\n=== Class Distribution ===")
data.groupBy("target").count().show()

print("\n=== Summary Statistics ===")
data.describe().show()

# Missing value check
print("\n=== Missing Values per Column ===")
data.select([count(when(col(c).isNull(), c)).alias(c) for c in data.columns]).show()

# === Step 4: Feature Preparation ===
# Convert target labels ('yes'/'no') to numeric
indexer = StringIndexer(inputCol="target", outputCol="label")
data = indexer.fit(data).transform(data)

# Select feature columns (excluding 'target' and 'label')
feature_cols = [c for c in data.columns if c not in ("target", "label")]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = assembler.transform(data)

# === Step 5: Split Dataset ===
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# === Step 6: Logistic Regression Model ===
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

# Train the model
lr_model = lr.fit(train_data)

# === Step 7: Make Predictions ===
predictions = lr_model.transform(test_data)
predictions.select("age", "chol", "thalach", "prediction", "label").show()

# === Step 8: Evaluate the Model ===
evaluator_acc = MulticlassClassificationEvaluator(metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(metricName="f1")

accuracy = evaluator_acc.evaluate(predictions)
f1_score = evaluator_f1.evaluate(predictions)

print(f"\nModel Accuracy: {accuracy:.3f}")
print(f"F1-Score: {f1_score:.3f}")

# === Step 9: Visualize Predicted vs Actual ===
pred_pd = predictions.select("prediction", "label").toPandas()

plt.figure(figsize=(5,4))
plt.scatter(pred_pd["label"], pred_pd["prediction"], color="orange")
plt.title("Predicted vs Actual (Heart Disease)")
plt.xlabel("Actual Label")
plt.ylabel("Predicted Label")
plt.grid(True)
plt.show()

# Stop Spark
spark.stop()
