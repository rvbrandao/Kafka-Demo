import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = 'order_confirmed'

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='user_notification'
    )

while True:
    for message in consumer:
        customer = json.loads(message.value.decode('utf-8'))
        print(f"Received order confirmed from: {customer['customer_email']}")                
        print(f"Sending email..")
        
        consumer.commit()
