"""
Kafka Producer for Weather Data
Sends temperature, humidity, and wind direction packed in 3 bytes
Format: 14 bits temp, 7 bits humidity, 3 bits direction
"""
from kafka import KafkaProducer
import time
import random
from datetime import datetime
import struct
import numpy as np

# Configure Kafka Producer (send raw bytes)
producer = KafkaProducer(
    bootstrap_servers='iot.redesuvg.cloud:9092',
)

topic_name = '21403'

DIRS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

print(f"Starting to produce weather data to topic: {topic_name}")
print("Format: 3 bytes (14 bits temp 0-100°C, 7 bits humidity 0-100%, 3 bits direction)\n")

def pack_weather_data(temp_c, humidity_pct, wind_deg):
    """
    Pack weather data into 3 bytes (24 bits)
    - Temperature: 14 bits (0-16383) -> Maps 0-100°C with 0.01°C resolution (0.00 to 99.99)
    - Humidity: 7 bits (0-127) -> Maps 0-100% as integers (0-100)
    - Wind: 3 bits (0-7) -> Maps to 8 directions (N, NE, E, SE, S, SW, W, NW)
    """
    # Scale temperature: 0-100°C with 2 decimals -> 0-10000 -> fit in 14 bits (0-16383)
    # Multiply by 100 to preserve 2 decimal places
    temp_scaled = int(temp_c * 100)
    temp_scaled = max(0, min(10999, temp_scaled))  # Clamp to 0-99.99 (0-9999)
    
    # Humidity: 0-100 integers, map to 7 bits (0-127)
    # Since we need 0-100, we can fit it directly (101 values fit in 7 bits = 128 values)
    humidity_scaled = int(humidity_pct)
    humidity_scaled = max(0, min(100, humidity_scaled))  # Clamp to 0-100
    
    # Convert wind direction to 3 bits (8 directions)
    # 0=N, 1=NE, 2=E, 3=SE, 4=S, 5=SW, 6=W, 7=NW
    wind_sector = int((wind_deg % 360) / 45) % 8
    
    # Pack into 24 bits: [14 bits temp][7 bits humidity][3 bits wind]
    packed = (temp_scaled << 10) | (humidity_scaled << 3) | wind_sector
    
    # Convert to 3 bytes
    byte1 = (packed >> 16) & 0xFF
    byte2 = (packed >> 8) & 0xFF
    byte3 = packed & 0xFF
    
    return bytes([byte1, byte2, byte3])

try:
    while True:
        # Generate sample weather data
        temperature = round(np.clip(np.random.normal(50.0, 30.0), 0, 110)*100)/100  # 0-100°C with 2 decimals
        humidity = round(np.clip(np.random.normal(40.0, 20.0), 0, 100))  # 0-100 as integer
        wind_direction = round(random.uniform(0, 360), 2)
        wind_sector = int((wind_direction % 360) / 45) % 8
        
        # Pack into 3 bytes
        packed_data = pack_weather_data(temperature, humidity, wind_direction)
        
        # Send to Kafka
        producer.send(topic_name, value=packed_data)
        
        print(f"Sent: Temp={temperature}°C, Humidity={humidity}%, Wind={DIRS[wind_sector]} -> {packed_data.hex()}")
        
        # Wait before sending next message
        time.sleep(10)
        
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.close()
    print("Producer closed")