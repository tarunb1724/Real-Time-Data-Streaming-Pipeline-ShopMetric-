import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Note: Using port 9092 because we are running outside Docker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUCTS = ["iPhone 15", "MacBook Air", "Samsung S24", "Sony XM5", "Dyson Vacuum", "Rolex Watch"]
STATUS_LIST = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED"] # 25% chance of cancellation

print("🛒 ShopMetric Generator Started... Press Ctrl+C to stop.")

def generate_order():
    return {
        "order_id": fake.uuid4(),
        "user_id": fake.random_int(100, 9999),
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 3),
        "amount": round(random.uniform(500, 3000), 2),
        "country": fake.country(),
        "status": random.choice(STATUS_LIST),
        "timestamp": int(time.time())
    }

while True:
    try:
        order = generate_order()
        producer.send('shop_orders', order)
        print(f"📦 Sent: {order['product']} (${order['amount']}) - {order['status']}")
        time.sleep(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(2)