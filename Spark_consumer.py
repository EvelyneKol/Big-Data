from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Αρχικοποίηση του Spark session
spark = SparkSession.builder \
    .appName("VehiclePositionProcessor") \
    .getOrCreate()

# Ρυθμίσεις του Kafka
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

# Ανάγνωση δεδομένων από το Kafka
lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic_name) \
    .load()

# Ανάλυση των δεδομένων JSON και εφαρμογή του σχήματος
lines = lines.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Εκτέλεση μετασχηματισμών για τον υπολογισμό του vcount και του vspeed
result = lines.groupBy("time", "link").agg(
    count("name").alias("vcount"),
    avg("speed").alias("vspeed")
)

# Εξαγωγή των αποτελεσμάτων
query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()  # Αναμονή για τερματισμό της ροής δεδομένων
