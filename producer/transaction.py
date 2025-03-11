import json

from kafka import KafkaProducer
from kafka import KafkaConsumer

ORDER_KAFKA_TOPIC = 'order_details'
ORDER_CONFIRMED_KAFKA_TOPIC = 'order_confirmed'

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers='localhost:29092',
    enable_auto_commit=False,  # Desativa o commit automático
    auto_offset_reset='earliest',
    group_id='transaction'
    )

producer = KafkaProducer(bootstrap_servers='localhost:29092')


print('Going start listening...')
while True:
    for message in consumer:
        order = json.loads(message.value.decode('utf-8'))
        print(order)
        
        data = {
            "customer_id": order['customer_id'],             
            "customer_email": f"{order['order_id']}@gmail.com",
            "total_cost": order['total_cost'] 
            }
        
        try:
            consumer.commit()

            producer.send(
                        ORDER_CONFIRMED_KAFKA_TOPIC,
                        value=json.dumps(data).encode('utf-8')
                        )
        except Exception as e:
            print(f"Error: {e}")            
            break  
        #break