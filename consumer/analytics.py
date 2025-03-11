import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = 'order_confirmed'

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='analytics'
    )

total_orders_count = 0
total_revenue = 0

print(f"Analytics listening..")

while True:
    for message in consumer:
        print(f"Updating analytics..")
        consumed_message = json.loads(message.value.decode('utf-8'))
        
        total_cost = float(consumed_message['total_cost'])
        total_orders_count +=1
        total_revenue += total_cost

        print(f"Orders so far today: {total_orders_count}")
        print(f"Revenue so far today: {total_revenue}")

        consumer.commit()