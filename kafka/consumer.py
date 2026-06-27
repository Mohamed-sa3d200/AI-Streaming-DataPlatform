from confluent_kafka import Consumer, KafkaError

# Kafka Consumer Configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'data-engineering-group',      # Consumer group name (vital for scaling & load balancing)
    'auto.offset.reset': 'earliest'            # Start reading from the beginning if no previous offset exists
}

# Create the Kafka Consumer instance
consumer = Consumer(consumer_config)
topic_name = 'customer_chats'

# Subscribe to the specified topic
consumer.subscribe([topic_name])

print(f"📥 Consumer is running and waiting for messages from [{topic_name}]...")

try:
    while True:
        # Poll for new messages every 1.0 second
        msg = consumer.poll(1.0)
        
        # Scenario 1: No message received within the timeout window
        if msg is None:
            continue
            
        # Scenario 2: An error occurred while fetching the message
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Reached the end of the current partition layout
                continue
            else:
                print(f"❌ Consumer error occurred: {msg.error()}")
                break

        # Scenario 3: Successfully received a valid message
        # Decode the raw bytes payload into a readable UTF-8 string
        data = msg.value().decode('utf-8')
        print(f"🔔 Received new message: {data} from Partition: [{msg.partition()}]")

except KeyboardInterrupt:
    print("\n🛑 Stopping the Consumer...")
finally:
    # Safely close the consumer to trigger rebalancing and free up Kafka resources
    consumer.close()