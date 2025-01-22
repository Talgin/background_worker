from confluent_kafka import Producer
import json

# Kafka producer configuration
conf = {
    'bootstrap.servers': '194.4.56.77:40005',  # Replace with your Kafka server
    'client.id': 'python-producer'
}

# Create Producer instance
producer = Producer(conf)

# TESTING FOR FACE
# video
# data = {'incident_id': '66d7ed386847cf186a15e256', 'timestamp': '01.01.01', 'media_type': 'video/mp4'}
# photo
# data = {'incident_id': '66d7ed916847cf186a15e257', 'timestamp': '01.01.01', 'media_type': 'image/jpg'}
# TESTING FOR LICENSE PLATE
# video
# END OF TESTING FOR FACE ------------------------------------------------------------------
# data = {'incident_id': '66da028281004fbd522b428d', 'timestamp': '01.01.01', 'media_type': 'video/mp4'}
# photo
# the following was commented on 28.12.2024 - while preparing rtsp
# data = {'incident_id': '66ddadf081004fbd522b428e', 'timestamp': '01.01.01', 'media_type': 'image/jpg'}
# RTSP
data = {'camera_id': '66ddadf081004fbd522b428e', 'rtsp_url': 'rtsp://admin:Asdf12345@192.168.1.108:554/cam/realmonitor?channel=1&subtype=0', 'timestamp': '28.12.2024', 'camera_name': 'first_0001'}
# Convert dictionary to JSON string
data_json = json.dumps(data)

# Define the Kafka topic for FACE
topic = 'camera_topic'
# Kafka topic for LPR
# topic = 'detection_alpr'

# Function to handle message delivery reports (optional but useful for error handling)
def delivery_callback(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Send the message to Kafka
producer.produce(topic, value=data_json, callback=delivery_callback)

# Wait for any outstanding messages to be delivered
producer.flush()

# producer.send('analytics_face', {'incident_id': '123456', 'timestamp': '01.01.01', 'media_type': 'image/jpg'})
