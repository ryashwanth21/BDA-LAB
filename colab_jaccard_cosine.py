from pyspark.sql import SparkSession
from itertools import combinations
import math

# Initialize Spark
spark = SparkSession.builder.appName("MusicCF").getOrCreate()

# Sample user-song interaction
data = spark.createDataFrame([
    (1, 101), (1, 102), (1, 103),
    (2, 101), (2, 104),
    (3, 102), (3, 103), (3, 104),
    (4, 101), (4, 103)
], ["user", "song"])

# === Step 1: Aggregate songs for each user ===
user_songs = data.groupBy("user").agg({"song": "collect_set"}).withColumnRenamed("collect_set(song)", "songs")
user_list = [(row.user, set(row.songs)) for row in user_songs.collect()]

# === Step 2: Define similarity functions ===
def jaccard_similarity(s1, s2):
    inter = len(s1 & s2)
    union = len(s1 | s2)
    return inter / union if union != 0 else 0.0

def cosine_similarity(s1, s2):
    inter = len(s1 & s2)
    norm = math.sqrt(len(s1)) * math.sqrt(len(s2))
    return inter / norm if norm != 0 else 0.0

# === Step 3: Compute similarity for all user pairs ===
print("User-User Similarities:\n")
for (u1, songs1), (u2, songs2) in combinations(user_list, 2):
    j_sim = jaccard_similarity(songs1, songs2)
    c_sim = cosine_similarity(songs1, songs2)
    print(f"Users {u1} & {u2} => Jaccard: {j_sim:.2f}, Cosine: {c_sim:.2f}")

spark.stop()
