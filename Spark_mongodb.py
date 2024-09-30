from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pymongo import MongoClient
import json

# Αρχικοποίηση του Spark Session
spark = SparkSession.builder \
    .appName("VehiclePositionProcessor") \
    .getOrCreate()

# Ρυθμίσεις για το Kafka
bootstrap_servers = "localhost:9092"
topic_name = "vehicle_positions"

# Ορισμός του σχήματος για τα εισερχόμενα δεδομένα JSON
schema = StructType([
    StructField("name", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("time", StringType(), True),
    StructField("link", StringType(), True),
    StructField("position", FloatType(), True),
    StructField("spacing", FloatType(), True),
    StructField("speed", FloatType(), True)
])

# Ανάγνωση των δεδομένων από το Kafka
lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic_name) \
    .load()

# Ανάλυση των δεδομένων JSON και εφαρμογή του σχήματος
parsed_lines = lines.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Εφαρμογή μετασχηματισμών για υπολογισμό των vcount και vspeed
result = parsed_lines.groupBy("time", "link").agg(
    count("name").alias("vcount"),
    avg("speed").alias("vspeed")
)

# Σύνδεση με τη βάση δεδομένων MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.vehiclesData_db
processed_collection = db.processed_vehicle_positions
raw_collection = db.raw_vehicle_positions

# Συνάρτηση για αποθήκευση κάθε batch επεξεργασμένων δεδομένων στη MongoDB
def save_to_mongo(batch_df, batch_id, collection):
    try:
        # Μετατροπή του DataFrame σε JSON και φόρτωση σε λεξικό
        batch_data = batch_df.toJSON().map(lambda j: json.loads(j)).collect()
        if batch_data:
            collection.insert_many(batch_data)
    except Exception as e:
        print(f"Σφάλμα στην save_to_mongo: {e}")

# Αποθήκευση των ακατέργαστων δεδομένων στη MongoDB
raw_query = parsed_lines.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, batch_id: save_to_mongo(batch_df, batch_id, raw_collection)) \
    .start()

# Αποθήκευση των επεξεργασμένων δεδομένων στη MongoDB
processed_query = result.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch_df, batch_id: save_to_mongo(batch_df, batch_id, processed_collection)) \
    .start()

# Αναμονή για την ολοκλήρωση των ροών δεδομένων
raw_query.awaitTermination()
processed_query.awaitTermination()