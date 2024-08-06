import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer
import pandas as pd

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'vehicle_positions'

# Load vehicle data from CSV file
df_vehicles = pd.read_csv('vehicles.csv')

# Initialize Kafka producer
producer = Producer({
    'bootstrap.servers': bootstrap_servers
})

# Function to handle delivery report from Kafka producer
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} at partition {msg.partition()} offset {msg.offset()}")

# Get the start time of the simulation
simulation_start_time = datetime.now()

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

# Run the simulation
N = 5  # Interval in seconds
for n in range(0, 3600, N):
    # Calculate the current time in the simulation
    current_time = simulation_start_time + timedelta(seconds=n)
    
    # Filter data to include only moving vehicles
    current_data = df_vehicles[df_vehicles['t'] == n]
    
    for _, row in current_data.iterrows():
        # Check if the vehicle is not waiting at the origin node
        if row['link'] != 'waiting_at_origin_node'and row['link'] != 'trip_end':
            # Convert row to JSON with timestamp
            vehicle_with_timestamp = row_to_json(row, current_time)
            
            print(f"Sending JSON data: {json.dumps(vehicle_with_timestamp, indent=2)}")
            
            # Serialize to JSON and encode to bytes
            serialized_value = json.dumps(vehicle_with_timestamp).encode('utf-8')
            
            # Send JSON data to Kafka topic
            while True:
                try:
                    producer.produce(topic_name, value=serialized_value, callback=delivery_report)
                    break  # Exit loop if successful
                except BufferError:
                    # If buffer is full, wait and try again
                    producer.poll(1)  # Wait for 1 second
                    continue
            
            # Poll for delivery reports
            producer.poll(0)
    
    # Optional: Add delay to allow buffer to clear
    time.sleep(0.1)

# Flush any remaining messages
producer.flush()
