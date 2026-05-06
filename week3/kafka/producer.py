from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# --- Producer setup ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("✅ Kafka producer started")
print("Sending real-time orders to 'olist-orders' topic...")
print("Press Ctrl+C to stop\n")

statuses = ['delivered', 'shipped', 'canceled', 'processing']
cities = ['Lagos', 'Cairo', 'Dubai', 'London', 'Nairobi', 'Accra', 'Abuja']
payment_types = ['credit_card', 'debit_card', 'voucher', 'bank_transfer']

order_count = 0

try:
    while True:
        order = {
            'order_id': f'live_{datetime.now().strftime("%Y%m%d%H%M%S")}_{random.randint(1000, 9999)}',
            'customer_id': f'cust_{random.randint(1, 500):04d}',
            'customer_city': random.choice(cities),
            'order_status': random.choice(statuses),
            'payment_type': random.choice(payment_types),
            'payment_value': round(random.uniform(10, 5000), 2),
            'timestamp': datetime.now().isoformat()
        }

        producer.send('olist-orders', value=order)
        order_count += 1
        print(f"📦 Order {order_count}: {order['order_id']} | {order['customer_city']} | £{order['payment_value']}")
        time.sleep(1)

except KeyboardInterrupt:
    print(f"\n✅ Producer stopped. Sent {order_count} orders.")
    producer.close()
