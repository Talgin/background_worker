from configs import Config
from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient

from PIL import Image, ImageDraw
from io import BytesIO
from base64 import b64decode

import os
import json
import numpy as np

# Init Kafka consumer
kafka_consumer = Consumer(
    {
        'bootstrap.servers': Config.KAFKA_BROKER,
        'group.id' : Config.KAFKA_GROUP,
        'auto.offset.reset': 'earliest'
    }
)

# Init MongoDB collection
mongo_client = MongoClient(Config.MONGO_CON_STRING)
mongo_db = mongo_client[Config.MONGO_DB]
mongo_face_collection = mongo_db[Config.MONGO_LPR_COLLECTION]

# Variables for dublicate checking
incident_id = -1
incident_license_plates = [] # ['641FSA02', '412ZXC01', ...] 
frame_cntr = 0


# checking Kafka message for errors
def msg_has_error(msg):
    if msg is None:
        return True
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f'End of offset {msg.topic()} [{msg.partition()}]')
        elif msg.error():
            raise KafkaException(msg.error())
        return True
    return False
    

# return values from Kafka message
def msg_values_is_incorrect(msg):
    content_id = msg.get('content_id')
    file_type = msg.get('file_type')
    timestamp = msg.get('timestamp')
    frame = msg.get('frame')  
    cars = msg.get('cars')

    if content_id == None or file_type == None or timestamp == None or frame == None or cars == None:
        print('******************** Incorrect input fields! ********************')
        print(msg)
        return True

    if type(content_id) != str or type(file_type) != str or type(timestamp) != str or type(frame) != str or type(cars) != list:
        print('******************** Incorrect input fields type! ********************')
        print(msg)
        return True

    if file_type != 'video' and file_type != 'image':
        print('******************** Incorrect value of "file_type" field! ********************')
        print(msg)
        return True
    
    # Need check other fields value

    for car in cars:
        license_plate = car.get('license_plate')
        coordinates = car.get('coordinates')
        if license_plate == None or coordinates == None:
            print('******************** Incorrect keys "cars" field! ********************')
            print(msg)
            return True
        if type(license_plate) != str or type(coordinates) != list:
            print('******************** Incorrect types of "faces" field values! ********************')
            print(msg)
            return True
        if len(license_plate) != 8:
            print('******************** Incorrect vector dimension! ********************')
            print(msg)
            return True
        
        # Need check 'coordinates' field

    
    return False


# Checking license_plate in incident_license_plates
# If has duplicate return True, else return False
def duplicate_checking(content_id, license_plate):
    global incident_id, incident_license_plates, frame_cntr

    if incident_id == content_id:
        if license_plate in incident_license_plates: True
    else: 
        incident_id = content_id
        incident_license_plates.clear()
        frame_cntr = 0
    incident_license_plates.append(license_plate)
    return False


# Save frame and crops in Local file storage - output/{content_id}
def save_crops(content_id, frame, cars, file_type):
    image_bytes =  b64decode(frame)
    # frame.decode('base64')
    image = Image.open(BytesIO(image_bytes))

    path = Config.OUTPUT_PATH
    path = path + '\\' + content_id
    if not os.path.exists(path): os.mkdir(path)
    path += '\\'

    global frame_cntr
    # frame_0.jpg
    # crop_0_0.jpg
    # crop_0_1.jpg
    # crop_1_0.jpg
    frame_name = 'frame_' + str(frame_cntr) + '.jpg'
    if file_type == 'video': image.save(path + frame_name)
    
    # if file_type == 'video': draw = ImageDraw.Draw(image)

    frame_width, frame_height = image.size
    crop_cntr = 0
    output_cars = []
    
    for car in cars:
        license_plate = car.get('license_plate')
        coordinates = car.get('coordinates')
        x, y, width, height = coordinates
        if frame_width < width or frame_height < height: 
            print('******************** Incorrect car coordinates! ********************')
            print(f'frame width - {frame_width}, frame height - {frame_height}')
            print(f'face coordinates: x - {x}, y - {y}, width - {width}, height - {height}')
            continue

        # if file_type == 'video': draw.rectangle((x, y, width, height), outline='yellow', width=2)

        crop_name = 'crop_' + str(frame_cntr) + '_' + str(crop_cntr) + '.jpg'
        cropped_image = image.crop((x, y, width, height))
        cropped_image.save(path + crop_name)

        output_cars.append({'crop_name': crop_name, 'license_plate': license_plate, 'coordinates': coordinates})
        crop_cntr += 1
    
    # if file_type == 'video': image.save(path + frame_name)

    frame_cntr += 1

    return frame_name, output_cars


# 'content_id': '641323', 
# 'file_type': 'video', 
# 'timestamp': '2024-07-28 20:56:26.679209',
# 'frame': base64_img_1 
# 'faces': [{'vector': [], 'coordinates': [x, y, width, height]}, {'vector': [], 'coordinates': [x, y, width, height]}...]


# Infinity loop with receiving msg from kafka topic 
# Check duplicate
# Save frame and crops in Local file storage - OUTPUT
# Save output in Mongo
def consume_and_save():
    try:
        kafka_consumer.subscribe([Config.KAFKA_LPR_TOPIC])
        i = 0
        while True:
            msg = kafka_consumer.poll(timeout=0.1)

            if msg_has_error(msg): continue
            print('****************************************************************')
            print(f'Message received: {i}')
            i+=1
            msg = json.loads(msg.value())

            # check field values from message
            if msg_values_is_incorrect(msg): continue

            content_id = msg.get('content_id')
            file_type = msg.get('file_type')
            timestamp = msg.get('timestamp')
            frame = msg.get('frame')  
            cars = msg.get('cars')

            new_car_flag = False
        
            for car in cars:
                license_plate = car.get('license_plate')
                coordinates = car.get('coordinates')

                if duplicate_checking(content_id, license_plate): continue
                new_car_flag = True

            # If there isn't a new car then skip the message
            if not new_car_flag: 
                print('Duplicate frame!')
                continue

            # Save frame, crops and return picture names
            frame_name, cars = save_crops(content_id, frame, cars, file_type)

            # Save dict in Mongo 
            output_data = {
                'content_id': content_id, 
                'file_type': file_type, 
                'frame': frame_name,
                'cars': cars
                }
            mongo_face_collection.insert_one(output_data)
                
            print('Message saved!')
            print(f'Content_id: {content_id}  \n')
            
    except KafkaException as e:
        print(f'Kafka error') # print(f"[KafkaException]: {e.strerror}, filename: {e.filename}")
    finally: 
        kafka_consumer.close()
        mongo_client.close()
        

# Init module
if __name__ == '__main__': 
    consume_and_save()

# INPUT

# 'content_id': '641323', 
# 'file_type': 'video', 
# 'timestamp': '2024-07-28 20:56:26.679209',
# 'frame': base64_img_1 
# 'cars': [
#     {'license_plate': '645ADS07', 'coordinates': [x, y, width, height]},
#     ...
# ]


#OUTPUT

# 'content_id' : 64512
# 'file_type': 'img'
# 'frame': frame_1.jpg
# 'cars' : [
#     {
#         'crop_name': 'crop_0.jpg'
#         'license_plate': '645ADS07',
#         'coordinates': [x, y, width, height]
#     }
# ]

# Сохранить координаты или сразу кадры с квадратом