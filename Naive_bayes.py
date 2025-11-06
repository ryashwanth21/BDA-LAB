from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# Initialize Spark
spark = SparkSession.builder.appName("MedicalNaiveBayes").getOrCreate()

# === Step 1: Load dataset ===
# Assume CSV: "symptoms", "label" (contagious/non-contagious)
data = spark.read.csv("symptoms.csv", header=True, inferSchema=True)

# === Step 2: Preprocessing ===
label_indexer = StringIndexer(inputCol="label", outputCol="labelIndex")
tokenizer = Tokenizer(inputCol="symptoms", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")

# === Step 3: Naive Bayes Model ===
nb = NaiveBayes(featuresCol="features", labelCol="labelIndex")

# === Step 4: Pipeline ===
pipeline = Pipeline(stages=[label_indexer, tokenizer, remover, hashingTF, idf, nb])

# Split data
train, test = data.randomSplit([0.8, 0.2], seed=42)

# Train model
model = pipeline.fit(train)

# Predictions
pred = model.transform(test)
pred.select("symptoms", "label", "prediction").show(5, truncate=False)

# === Step 5: Evaluation ===
evaluator_precision = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction", metricName="precisionByLabel")
evaluator_recall = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction", metricName="recallByLabel")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction", metricName="f1")

precision = evaluator_precision.evaluate(pred)
recall = evaluator_recall.evaluate(pred)
f1 = evaluator_f1.evaluate(pred)

print(f"Precision: {precision:.3f}")
print(f"Recall: {recall:.3f}")
print(f"F1-score: {f1:.3f}")

spark.stop()
