from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json
import time
from kafka import KafkaConsumer
import uuid

def create_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = KafkaAdminClient(
        bootstrap_servers='localhost:9092',  # Update with your Kafka server address
        client_id='test-admin'
    )

    # Define a new topic
    topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]

    try:
        # Create the topic
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred while creating the topic: {e}")
    finally:
        admin_client.close()


# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Update with your Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON format
)

# Produce messages
def produce_order(order):
    producer.send("orders", value=order)
    print(f"Order Produced: {order}")

def produce_messages():
    for i in range(5):
        order = {
            "order_id": str(uuid.uuid4()),
            "user_id": f"user_{i}",
            "product_id": f"product_{i}",
            "quantity": 1,
            "status": "NEW"
        }
        produce_order(order)
    producer.flush()

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "orders",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='inventory-service',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def update_inventory(order):
    print(f"Updating inventory for order: {order['order_id']}")
    # Simulate inventory update
    print(f"Product {order['product_id']} stock reduced by {order['quantity']}.")

# Consume messages
def consume_messages():
    print("Inventory Service is listening for orders...")
    for message in consumer:
        update_inventory(message.value)

if __name__ == "__main__":
    create_topic("orders", num_partitions=3, replication_factor=1)
    produce_messages()
    consume_messages()
