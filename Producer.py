import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer
import pandas as pd

# Ρυθμίσεις του Kafka broker
bootstrap_servers = 'localhost:9092'
topic_name = 'vehicle_positions'

# Φόρτωση των δεδομένων οχημάτων από το αρχείο CSV
df_vehicles = pd.read_csv('vehicles.csv')

# Αρχικοποίηση του Kafka producer
producer = Producer({
    'bootstrap.servers': bootstrap_servers
})


# Συνάρτηση για την επεξεργασία της αναφοράς παράδοσης από τον Kafka producer
def delivery_report(err, msg):
    if err is not None:
        print(f"Αποτυχία παράδοσης μηνύματος: {err}")
    else:
        print(f"Το μήνυμα παραδόθηκε στο topic {msg.topic()}.")


# Λήψη της ώρας έναρξης της προσομοίωσης
simulation_start_time = datetime.now()


# Συνάρτηση που μετατρέπει μια γραμμή δεδομένων σε JSON μορφή με χρονική σήμανση
def row_to_json(row, current_time):
    return {
        "name": str(row['name']),
        "origin": str(row['orig']),
        "destination": str(row['dest']),
        "time": current_time.strftime('%d/%m/%Y %H:%M:%S'),
        "link": str(row['link']),
        "position": float(row['x']),
        "spacing": float(row['s']),
        "speed": float(row['v'])
    }


# Εκτέλεση της προσομοίωσης
N = 5  # Διάστημα σε δευτερόλεπτα
for n in range(0, 3600, N):
    # Υπολογισμός της τρέχουσας ώρας στην προσομοίωση
    current_time = simulation_start_time + timedelta(seconds=n)

    # Φιλτράρισμα δεδομένων για να συμπεριληφθούν μόνο τα κινούμενα οχήματα
    current_data = df_vehicles[df_vehicles['t'] == n]

    for _, row in current_data.iterrows():
        # Έλεγχος αν το όχημα δεν περιμένει στον κόμβο εκκίνησης
        if row['link'] != 'waiting_at_origin_node' and row['link'] != 'trip_end':
            # Μετατροπή της γραμμής σε JSON με χρονική σήμανση
            vehicle_with_timestamp = row_to_json(row, current_time)

            print(f"Αποστολή δεδομένων JSON: {json.dumps(vehicle_with_timestamp, indent=2)}")

            # Σειριοποίηση σε JSON και κωδικοποίηση σε bytes
            serialized_value = json.dumps(vehicle_with_timestamp).encode('utf-8')

            # Αποστολή των δεδομένων JSON στο Kafka topic
            producer.produce(topic_name, value=serialized_value, callback=delivery_report)

            # Πολιτική για αναφορές παράδοσης
            producer.poll(0)

    # Προσθήκη καθυστέρησης για να επιτραπεί η εκκαθάριση του buffer
    time.sleep(0.1)

# Εκκαθάριση τυχόν υπολειπόμενων μηνυμάτων
producer.flush()