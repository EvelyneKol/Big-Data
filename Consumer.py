from confluent_kafka import Consumer, KafkaException
import json

# Ρυθμίσεις του Kafka broker
bootstrap_servers = 'localhost:9092'
topic_name = 'vehicle_positions'

# Αρχικοποίηση του Kafka consumer
consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'vehicles',
    'auto.offset.reset': 'earliest'  # Επανεκκίνηση ανάγνωσης από το αρχικό μήνυμα αν δεν υπάρχουν offsets
})

# Εγγραφή στο topic
consumer.subscribe([topic_name])

# Συνάρτηση για κατανάλωση μηνυμάτων
def consume_messages():
    print(f"Κατανάλωση μηνυμάτων από το topic '{topic_name}'...")
    try:
        no_message_count = 0  # Μετρητής για το πλήθος των διαδοχικών φορών χωρίς μηνύματα
        max_no_message_count = 5  # Αριθμός διαδοχικών φορών χωρίς μηνύματα πριν τερματιστεί η κατανάλωση

        while True:
            msg = consumer.poll(timeout=1.0)  # Αναμονή για μήνυμα με timeout 1 δευτερόλεπτο
            if msg is None:  # Αν δεν υπάρχει μήνυμα
                no_message_count += 1
                if no_message_count >= max_no_message_count:  # Αν ξεπεραστεί το όριο
                    print("Δεν υπάρχουν άλλα μηνύματα για κατανάλωση.")
                    break  # Έξοδος από τον βρόχο
                continue

            no_message_count = 0  # Μηδενισμός του μετρητή αν ληφθεί μήνυμα

            if msg.error():  # Έλεγχος για σφάλμα στο μήνυμα
                if msg.error().code() == KafkaException._PARTITION_EOF:  # Αν έφτασε το τέλος του partition
                    print(f"Έφτασε το τέλος του partition {msg.partition()}")
                else:
                    print(f"Σφάλμα: {msg.error()}")
            else:
                # Αποκωδικοποίηση του ληφθέντος μηνύματος
                try:
                    message_content = json.loads(msg.value().decode('utf-8'))  # Αποκωδικοποίηση από JSON
                    print(f"Ληφθέν μήνυμα: {json.dumps(message_content, indent=2)}")
                except json.JSONDecodeError as e:
                    print(f"Αποτυχία αποκωδικοποίησης του JSON μηνύματος: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        # Κλείσιμο του consumer για να καθαρίσουν οι πόροι
        consumer.close()
# Ξεκινά η κατανάλωση μηνυμάτων
consume_messages()