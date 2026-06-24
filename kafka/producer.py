import json
import time
import random
from datetime import datetime
from faker import Faker

# Initialize Faker to generate realistic synthetic data
fake = Faker()


def generate_event():
    """
    Generate a single customer support event.

    Some events are intentionally corrupted to simulate
    real-world data quality issues for testing purposes.
    """

    current_time = datetime.now().isoformat()

    # Generate a random number between 0 and 1
    # to control the probability of bad records
    dice_roll = random.random()

    if dice_roll < 0.05:
        # 5% chance: Missing user_id (null value)
        return {
            "user_id": None,
            "message": fake.sentence(nb_words=5),
            "rating": random.randint(1, 5),
            "event_time": current_time
        }

    elif dice_roll < 0.10:
        # 5% chance: Invalid rating outside the accepted range
        return {
            "user_id": random.randint(1000, 9999),
            "message": fake.sentence(nb_words=5),
            "rating": 100,
            "event_time": current_time
        }

    elif dice_roll < 0.12:
        # 2% chance: Empty message field
        return {
            "user_id": random.randint(1000, 9999),
            "message": "",
            "rating": random.randint(1, 5),
            "event_time": current_time
        }

    else:
        # 88% chance: Valid customer support event
        return {
            "user_id": random.randint(1000, 9999),
            "message": f"Customer Issue: {fake.sentence(nb_words=6)}",
            "rating": random.randint(1, 5),
            "event_time": current_time
        }


def main():
    """
    Main loop that continuously generates events
    to simulate a real-time streaming source.
    """

    print("🚀 Starting data generator... Press Ctrl+C to stop.")

    try:
        while True:

            # Generate a new event
            event = generate_event()

            # Convert Python dictionary to JSON string
            json_payload = json.dumps(event)

            # Print the generated event
            print(f"[GENERATED] {json_payload}")

            # Wait for 1 second before generating the next event
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Data generator stopped successfully.")


if __name__ == "__main__":
    main()