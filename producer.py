"""
Kafka Producer for Weather Data
Sends temperature, humidity, and wind direction to Kafka topic
"""
from kafka import KafkaProducer
import json
import time
import numpy as np

# Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='iot.redesuvg.cloud:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = '21403'
DIRS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

print(f"Starting to produce weather data to topic: {topic_name}")

try:
    while True:
        # Generate sample weather data
        weather_data = {
            'temperature': round(np.clip(np.random.normal(50.0, 30.0), 0, 110)*100)/100, 
            'humidity': round(np.clip(np.random.normal(40.0, 20.0), 0, 100)),      
            'wind_direction': np.random.choice(DIRS)   
            # 'wind_direction': round(np.random.uniform(0, 360))
        }
        
        # Send to Kafka
        producer.send(topic_name, value=weather_data)
        print(f"Sent: {weather_data}")
        
        # Wait before sending next message
        time.sleep(10)
        
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.close()
    print("Producer closed")