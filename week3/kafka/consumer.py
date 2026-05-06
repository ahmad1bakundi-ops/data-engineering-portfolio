from kafka import KafkaConsumer
import json
from datetime import datetime

# --- Consumer setup ---
consumer = KafkaConsumer(
    'olist-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("✅ Kafka consumer started")
print("Listening for real-time orders...\n")

order_count = 0
total_revenue = 0
city_counts = {}

try:
    for message in consumer:
        order = message.value
        order_count += 1
        total_revenue += order['payment_value']

        city = order['customer_city']
        city_counts[city] = city_counts.get(city, 0) + 1

        print(f"✅ Received order {order_count}: {order['order_id']}")
        print(f"   City: {order['customer_city']} | Payment: {order['payment_type']} | Value: £{order['payment_value']}")
        print(f"   Running total: {order_count} orders | £{round(total_revenue, 2)} revenue")
        print(f"   Orders by city: {dict(sorted(city_counts.items(), key=lambda x: x[1], reverse=True))}")
        print()

except KeyboardInterrupt:
    print(f"\n✅ Consumer stopped.")
    print(f"Total orders processed: {order_count}")
    print(f"Total revenue: £{round(total_revenue, 2)}")
    print(f"Orders by city: {city_counts}")
    consumer.close()
