import json
import time
import random
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

# Initialize Faker to generate realistic dummy data
fake = Faker()

# Kafka configuration mapping to the local Docker broker
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
}

# Create the Kafka Producer instance
producer = Producer(kafka_config)
topic_name = 'customer_chats'

def delivery_report(err, msg):
    """
    Callback function to guarantee delivery status.
    Invoked once the message is acknowledged or failed.
    """
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to [Topic: {msg.topic()}] | Partition: [{msg.partition()}]")

def generate_event():
    """
    Simulates user chat events with intentional data quality anomalies.
    """
    current_time = datetime.now().isoformat()
    dice_roll = random.random()
    
    # Scenario 1: Missing User ID (Anomalous Data) - 5% chance
    if dice_roll < 0.05:
        return {
            "user_id": None, 
            "message": fake.sentence(nb_words=5), 
            "rating": random.randint(1, 5), 
            "event_time": current_time
        }
    # Scenario 2: Out-of-bounds Rating (Anomalous Data) - 5% chance
    elif dice_roll < 0.10:
        return {
            "user_id": random.randint(1000, 9999), 
            "message": fake.sentence(nb_words=5), 
            "rating": 100, 
            "event_time": current_time
        }
    # Scenario 3: Empty Message (Anomalous Data) - 2% chance
    elif dice_roll < 0.12:
        return {
            "user_id": random.randint(1000, 9999), 
            "message": "", 
            "rating": random.randint(1, 5), 
            "event_time": current_time
        }
    # Scenario 4: Clean, valid customer support issue - 88% chance
    else:
        return {
            "user_id": random.randint(1000, 9999), 
            "message": f"Customer Issue: {fake.sentence(nb_words=6)}", 
            "rating": random.randint(1, 5), 
            "event_time": current_time
        }

def main():
    print("🚀 Starting Kafka Producer and streaming data...")
    try:
        while True:
            # 1. Generate a new dictionary event
            event = generate_event()
            
            # 2. Convert dictionary to JSON string
            json_payload = json.dumps(event)
            
            # 3. Send raw data bytes to Kafka with a delivery callback
            producer.produce(
                topic=topic_name, 
                value=json_payload.encode('utf-8'), 
                callback=delivery_report
            )
            
            # 4. Trigger internal event queues (non-blocking)
            producer.poll(0)
            
            # 5. Wait for 1 second before generating the next event
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping the Producer...")
    finally:
        # 6. Force flush any remaining messages in the internal buffer
        producer.flush()

if __name__ == "__main__":
    main()