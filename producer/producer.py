# kafka_producer.py

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import json
import time

# 1. Configure Schema Registry
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 2. Read the registered schema from Schema Registry
subject = 'orders-value'
schema_str = schema_registry_client.get_latest_version(subject).schema.schema_str

# 3. Create Avro Serializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# 4. Configure Kafka Producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Ensure Kafka broker is listening on localhost
    'key.serializer': StringSerializer('utf_8'),  # Serializer for key
    'value.serializer': avro_serializer           # Serializer for value (Avro)
}
producer = SerializingProducer(producer_conf)

# 5. Create Order Data
order = {
    "order_id": "12345",
    "customer_id": "C003",
    "amount": 300.5,
    "order_date": "2024-12-25"
}

# 6. Delivery Callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# 7. Produce Messages
for i in range(5):
    order['order_id'] = f"1000{i}"
    order['amount'] = 100 + i * 50
    producer.produce(topic='orders', key=order['order_id'], value=order, on_delivery=delivery_report)
    time.sleep(1)  # Simulate sending data every second

producer.flush()  # Ensure all messages are sent
