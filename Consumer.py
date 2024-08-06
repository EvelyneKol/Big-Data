from confluent_kafka import Consumer, KafkaException
import json

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'vehicle_positions'

# Initialize Kafka consumer
consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'example-group',
    'auto.offset.reset': 'earliest'
})

# Subscribe to the topic
consumer.subscribe([topic_name])

# Function to consume messages
def consume_messages():
    print(f"Consuming messages from topic '{topic_name}'...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print(f"Reached end of partition {msg.partition()}")
                else:
                    print(f"Error: {msg.error()}")
            else:
                # Print the received message
                print(f"Received message: {json.loads(msg.value().decode('utf-8'))}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Start consuming messages
consume_messages()