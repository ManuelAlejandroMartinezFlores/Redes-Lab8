"""
Simple Real-time Weather Data Plotter
Clears and redraws plots periodically
"""
import matplotlib
matplotlib.use('TkAgg')

from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from collections import deque
from datetime import datetime
import time
import ast

# Configure Kafka Consumer
consumer = KafkaConsumer(
    '21403',
    bootstrap_servers='iot.redesuvg.cloud:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

DIRS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
DIRS = {d: i*45 for i, d in enumerate(DIRS)}

# Data storage (keep last 50 points)
max_points = 50
timestamps = deque(maxlen=max_points)
temperatures = deque(maxlen=max_points)
humidity_values = deque(maxlen=max_points)
wind_directions = deque(maxlen=max_points)

print("Starting simple real-time plotter...")
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
                data = record.value
                timestamps.append(len(temperatures))
                temperatures.append(float(data['temperature']))
                humidity_values.append(int(data['humidity']))
                wind_directions.append(DIRS[data['wind_direction']])
                print(f"Data: Temp={data['temperature']}°C, Humidity={data['humidity']}%, Wind={data['wind_direction']}°")
        
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
            ax1.set_ylim(0, 110)
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
            
            plt.tight_layout()
            plt.draw()
            plt.pause(5)

except KeyboardInterrupt:
    print("\nStopping plotter...")
finally:
    consumer.close()
    plt.close('all')
    print("Plotter closed")