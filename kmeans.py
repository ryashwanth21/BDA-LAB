from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt

# Initialize Spark
spark = SparkSession.builder.appName("CustomerSegmentation").getOrCreate()

# Sample customer data: age, income, spending score
data = spark.createDataFrame([
    (25, 50000, 60),
    (45, 80000, 30),
    (30, 60000, 50),
    (35, 65000, 55),
    (50, 90000, 20),
    (23, 48000, 70),
    (40, 70000, 40),
    (27, 52000, 65)
], ["age", "income", "spending_score"])

# === Step 1: Assemble features ===
assembler = VectorAssembler(inputCols=["age","income","spending_score"], outputCol="features")
data = assembler.transform(data)

# === Step 2: Fit KMeans and test different k ===
k_values = [2, 3, 4]
inertia = []  # Sum of squared distances

for k in k_values:
    kmeans = KMeans(featuresCol="features", k=k, seed=42)
    model = kmeans.fit(data)
    predictions = model.transform(data)
    
    # Compute sum of squared distances (inertia)
    inertia.append(model.summary.trainingCost)
    
    print(f"\n=== Clustering Results for k={k} ===")
    predictions.select("age","income","spending_score","prediction").show()

# === Step 3: Plot inertia vs k ===
plt.figure(figsize=(5,4))
plt.plot(k_values, inertia, marker='o')
plt.xlabel("Number of Clusters (k)")
plt.ylabel("Sum of Squared Distances (Inertia)")
plt.title("Elbow Method for Optimal k")
plt.show()

spark.stop()
