from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
import random

def create_topics():
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='movie_admin')

    topics = [
        NewTopic(name="ticket_bookings", num_partitions=3, replication_factor=1),
        NewTopic(name="ticket_payments", num_partitions=3, replication_factor=1),
        NewTopic(name="ticket_notifications", num_partitions=3, replication_factor=1)
    ]

    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        print("Topics created successfully!")
    except Exception as e:
        print(f"Error creating topics: {e}")
    finally:
        admin_client.close()


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Movie Booking Service (Producer)

def produce_booking_request(movie_id, user_id, num_tickets):
    booking_request = {
        "booking_id": str(uuid.uuid4()),
        "movie_id": movie_id,
        "user_id": user_id,
        "num_tickets": num_tickets,
        "status": "PENDING"
    }
    producer.send("ticket_bookings", value=booking_request)
    print(f"Booking request sent: {booking_request}")

# Inventory Service (Consumer)

consumer = KafkaConsumer(
    "ticket_bookings",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='inventory_service',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)


# Simulated inventory
inventory = {
    "movie_1": 50,
    "movie_2": 30,
    "movie_3": 20
}

def process_booking(booking):
    movie_id = booking["movie_id"]
    num_tickets = booking["num_tickets"]

    if inventory.get(movie_id, 0) >= num_tickets:
        inventory[movie_id] -= num_tickets
        booking["status"] = "RESERVED"
        print(f"Reserved tickets for booking: {booking['booking_id']}")
        producer.send("ticket_payments", value=booking)
    else:
        booking["status"] = "FAILED"
        print(f"Booking failed (insufficient tickets): {booking['booking_id']}")


# Payment Service (Consumer)

consumer = KafkaConsumer(
    "ticket_payments",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='payment_service',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def process_payment(booking):
    # Simulate successful payment
    booking["status"] = "PAID"
    print(f"Payment processed for booking: {booking['booking_id']}")
    producer.send("ticket_notifications", value=booking)

# Notification Service (Consumer)


consumer = KafkaConsumer(
    "ticket_notifications",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='notification_service',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def send_notification(booking):
    if booking["status"] == "PAID":
        print(f"Notification sent to user {booking['user_id']}: Booking confirmed for {booking['movie_id']}!")
    else:
        print(f"Notification sent to user {booking['user_id']}: Booking failed.")



if __name__ == "__main__":
    create_topics()

    movies = ["movie_1", "movie_2", "movie_3"]
    for i in range(5):  # Simulating 5 bookings
        produce_booking_request(
            movie_id=random.choice(movies),
            user_id=f"user_{i}",
            num_tickets=random.randint(1, 5)
        )
    producer.flush()

    print("Inventory Service listening for bookings...")
    for message in consumer:
        process_booking(message.value)

    print("Payment Service listening for payments...")
    for message in consumer:
        process_payment(message.value)

    print("Notification Service listening for notifications...")
    for message in consumer:
        send_notification(message.value)



# # Produce messages with custom partitioning
# def produce_with_custom_partitioner():
#     for i in range(5):
#         key = f"key-{i}"  # Custom key that determines the partition
#         value = {"message": f"Message {i}"}
#         producer.send("custom", key=key, value=value)
#         print(f"Produced message: {value} with key: {key}")
#     producer.flush()

# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     key_serializer=lambda k: k.encode('utf-8'),
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )
