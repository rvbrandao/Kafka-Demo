import json
import time
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = 'order_details'
ORDER_LIMIT = 100
KAFKA_SERVER = 'localhost:29092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

print('Going to be generating orders...')
for i in range(1, ORDER_LIMIT):
    order = {
        'order_id': i,
        'customer_id': f"tom_{i}",
        'total_cost': i * 2,
        'order_time': time.time()
    }
    producer.send(
        ORDER_KAFKA_TOPIC, 
        value=json.dumps(order).encode('utf-8')
        )
    print(f'Order {i} sent to Kafka')
    time.sleep(5)