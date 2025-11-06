# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LinearSVC
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Initialize Spark session
spark = SparkSession.builder.appName("SpamClassification").getOrCreate()

# === Step 1: Load the Data ===
# Sample CSV format: "label,text"
# label = "spam" or "ham"
data = spark.createDataFrame([
    ("spam", "Win money now! Click here to claim your prize"),
    ("ham", "Hey, are we still meeting for lunch today?"),
    ("spam", "Exclusive offer just for you, act now!"),
    ("ham", "Can you send me the report by tomorrow?"),
    ("spam", "Congratulations, you have won a free vacation!"),
    ("ham", "Let’s catch up later this evening.")
], ["label", "text"])

# === Step 2: Label Encoding ===
indexer = StringIndexer(inputCol="label", outputCol="labelIndex")

# === Step 3: Text Preprocessing ===
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")

# === Step 4: SVM Model ===
svm = LinearSVC(featuresCol="features", labelCol="labelIndex")

# === Step 5: Create Pipeline ===
pipeline = Pipeline(stages=[indexer, tokenizer, remover, hashingTF, idf, svm])

# === Step 6: Parameter Tuning for Regularization ===
paramGrid = ParamGridBuilder() \
    .addGrid(svm.regParam, [0.01, 0.1, 1.0]) \
    .build()

# === Step 7: Evaluator and Cross-Validation ===
evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="labelIndex", predictionCol="prediction", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="labelIndex", predictionCol="prediction", metricName="f1")

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator_f1,
                          numFolds=3)

# === Step 8: Train the Model ===
cvModel = crossval.fit(data)

# === Step 9: Predictions ===
predictions = cvModel.transform(data)
predictions.select("text", "label", "prediction").show(truncate=False)

# === Step 10: Evaluate the Model ===
accuracy = evaluator_acc.evaluate(predictions)
f1_score = evaluator_f1.evaluate(predictions)

print(f"Accuracy = {accuracy:.3f}")
print(f"F1-score = {f1_score:.3f}")

# === Step 11: Best Model Regularization Parameter ===
bestModel = cvModel.bestModel.stages[-1]
print(f"Best Regularization Parameter (regParam): {bestModel._java_obj.getRegParam()}")

# Stop Spark session
spark.stop()
