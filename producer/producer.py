#!/usr/bin/env python
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
import time
import json

schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

subject_name = "orders2-value"
schema_response = schema_registry_client.get_latest_version(subject_name)
order_schema = schema_response.schema.schema_str

json_serializer = JSONSerializer(
    order_schema,
    schema_registry_client,
    to_dict=lambda x, ctx: x
)

producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': json_serializer
}
producer = SerializingProducer(producer_conf)

def send_order(order):
    try:
        # Không encode dưới dạng byte
        producer.produce(
            topic='orders2',
            key=order['customer_id'],
            value=order,  # Truyền object trực tiếp
            on_delivery=lambda err, msg: print(f"Message {'failed' if err else 'sent'} to {msg.topic()}")
        )
    except Exception as e:
        print(f"Failed to send: {e}")

# Gửi dữ liệu
for i in range(5):
    order = {
        "customer_id": f"CUST-{i}",
        "amount": 100 + i * 50,
        "order_date": f"2024-12-{25 + i}"
    }
    send_order(order)
    time.sleep(1)

producer.flush()
print("Done producing JSON messages.")
