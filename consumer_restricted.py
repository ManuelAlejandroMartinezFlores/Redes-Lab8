"""
Simple Real-time Weather Data Plotter
Unpacks 3-byte weather data and plots it
Format: 14 bits temp, 7 bits humidity, 3 bits direction
"""
import matplotlib
matplotlib.use('TkAgg')

from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from collections import deque
from datetime import datetime
import time

# Configure Kafka Consumer (no deserializer, we handle raw bytes)
consumer = KafkaConsumer(
    '21403',
    bootstrap_servers='iot.redesuvg.cloud:9092'
)

def unpack_weather_data(packed_bytes):
    """
    Unpack 3 bytes into weather data
    - Temperature: 14 bits -> 0-100°C with 2 decimal places (0.00 to 99.99)
    - Humidity: 7 bits -> 0-100% as integers
    - Wind: 3 bits -> 8 directions
    """
    if len(packed_bytes) != 3:
        raise ValueError("Expected 3 bytes")
    
    # Convert bytes to 24-bit integer
    packed = (packed_bytes[0] << 16) | (packed_bytes[1] << 8) | packed_bytes[2]
    
    # Extract fields
    temp_scaled = (packed >> 10) & 0x3FFF  # 14 bits
    humidity_scaled = (packed >> 3) & 0x7F  # 7 bits
    wind_sector = packed & 0x07  # 3 bits
    
    # Unscale values
    temperature = temp_scaled / 100.0  # Divide by 100 to get 2 decimal places
    humidity = humidity_scaled  # Already 0-100 integer
    
    # Convert wind sector to degrees (center of each sector)
    wind_degrees = wind_sector * 45.0  # Centers: 22.5, 67.5, 112.5, etc.
    
    # Wind direction names
    directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    wind_name = directions[wind_sector]
    
    return temperature, humidity, wind_degrees, wind_name

# Data storage (keep last 50 points)
max_points = 50
timestamps = deque(maxlen=max_points)
temperatures = deque(maxlen=max_points)
humidity_values = deque(maxlen=max_points)
wind_directions = deque(maxlen=max_points)

print("Starting simple real-time plotter...")
print("Format: 3 bytes (14 bits temp 0-100°C, 7 bits humidity 0-100%, 3 bits direction)")
print("Waiting for data from Kafka...\n")

plt.ion()  # Interactive mode
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10))
fig.suptitle('Real-Time Weather Data', fontsize=16, fontweight='bold')

try:
    while True:
        # Collect messages for 1 second
        messages = consumer.poll(timeout_ms=1000)
        
        # Add new data
        for topic_partition, records in messages.items():
            for record in records:
                try:
                    # Unpack the 3 bytes
                    packed_bytes = record.value
                    temp, hum, wind_deg, wind_name = unpack_weather_data(packed_bytes)
                    
                    timestamps.append(len(temperatures))
                    temperatures.append(temp)
                    humidity_values.append(hum)
                    wind_directions.append(wind_deg)
                    
                    print(f"Data: Temp={temp:.2f}°C, Humidity={hum}%, Wind={wind_deg:.1f}° ({wind_name}) <- {packed_bytes.hex()}")
                except Exception as e:
                    print(f"Error unpacking data: {e}")
        
        # Only plot if we have data
        if len(timestamps) > 0:
            # Clear all axes
            ax1.clear()
            ax2.clear()
            ax3.clear()
            
            # Recreate plots
            x_indices = list(range(len(timestamps)))
            time_labels = [ts for ts in timestamps]
            
            # Temperature plot
            ax1.plot(x_indices, list(temperatures), 'r-', linewidth=2)
            ax1.set_ylabel('Temperature (°C)', fontsize=12)
            ax1.set_ylim(0, 100)
            ax1.grid(True, alpha=0.3)
            ax1.set_title('Temperature', fontsize=10)
            
            # Humidity plot
            ax2.plot(x_indices, list(humidity_values), 'b-', linewidth=2)
            ax2.set_ylabel('Humidity (%)', fontsize=12)
            ax2.set_ylim(0, 100)
            ax2.grid(True, alpha=0.3)
            ax2.set_title('Humidity', fontsize=10)
            
            # Wind direction plot
            ax3.scatter(x_indices, list(wind_directions), c='green', s=50, alpha=0.6)
            ax3.set_ylabel('Wind Direction (°)', fontsize=12)
            ax3.set_xlabel('Time', fontsize=12)
            ax3.set_ylim(0, 360)
            ax3.set_yticks([0, 90, 180, 270, 360])
            ax3.set_yticklabels(['N', 'E', 'S', 'W', 'N'])
            ax3.grid(True, alpha=0.3)
            ax3.set_title('Wind Direction', fontsize=10)
            
            # Set x-axis labels (show first and last)
            # if len(timestamps) > 1:
            #     for ax in [ax1, ax2, ax3]:
            #         ax.set_xticks([0, len(timestamps)-1])
            #         ax.set_xticklabels([time_labels[0], time_labels[-1]])
            
            plt.tight_layout()
            plt.draw()
            plt.pause(0.01)

except KeyboardInterrupt:
    print("\nStopping plotter...")
finally:
    consumer.close()
    plt.close('all')
    print("Plotter closed")