# Import libraries
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("BankTermDepositPrediction").getOrCreate()

# === Step 1: Load the dataset ===
# Example CSV format: age,job,marital,education,default,balance,housing,loan,contact,day,month,duration,campaign,pdays,previous,poutcome,y
data = spark.read.csv("bank.csv", header=True, inferSchema=True)

# Display schema
data.printSchema()

# === Step 2: Label encode categorical columns ===
categorical_cols = [col for col, dtype in data.dtypes if dtype == 'string' and col != 'y']
indexers = [StringIndexer(inputCol=column, outputCol=column + "_index", handleInvalid="keep") 
            for column in categorical_cols]

# Encode target variable
label_indexer = StringIndexer(inputCol="y", outputCol="label", handleInvalid="keep")

# === Step 3: Assemble features ===
feature_cols = [col + "_index" if col in categorical_cols else col for col in data.columns if col != 'y']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# === Step 4: Split the data ===
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# === Step 5: Decision Tree Classifier ===
dt = DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=5)

# === Step 6: Random Forest Classifier ===
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=5)

# === Step 7: Build pipelines ===
from pyspark.ml import Pipeline
pipeline_dt = Pipeline(stages=indexers + [label_indexer, assembler, dt])
pipeline_rf = Pipeline(stages=indexers + [label_indexer, assembler, rf])

# === Step 8: Train the models ===
model_dt = pipeline_dt.fit(train_data)
model_rf = pipeline_rf.fit(train_data)

# === Step 9: Make predictions ===
pred_dt = model_dt.transform(test_data)
pred_rf = model_rf.transform(test_data)

# === Step 10: Evaluate performance ===
evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

# Decision Tree metrics
acc_dt = evaluator_acc.evaluate(pred_dt)
f1_dt = evaluator_f1.evaluate(pred_dt)

# Random Forest metrics
acc_rf = evaluator_acc.evaluate(pred_rf)
f1_rf = evaluator_f1.evaluate(pred_rf)

print("=== Decision Tree Results ===")
print(f"Accuracy: {acc_dt:.3f}")
print(f"F1-score: {f1_dt:.3f}\n")

print("=== Random Forest Results ===")
print(f"Accuracy: {acc_rf:.3f}")
print(f"F1-score: {f1_rf:.3f}")

# === Step 11: Compare Results with Plot ===
models = ['Decision Tree', 'Random Forest']
accuracy = [acc_dt, acc_rf]
f1_scores = [f1_dt, f1_rf]

plt.figure(figsize=(6,4))
plt.bar(models, accuracy, color='skyblue', label='Accuracy')
plt.bar(models, f1_scores, color='orange', alpha=0.7, label='F1-score')
plt.title("Decision Tree vs Random Forest Performance")
plt.ylabel("Score")
plt.legend()
plt.show()

# Stop Spark
spark.stop()
