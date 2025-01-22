from configs import Config
from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient
from pymilvus import MilvusClient
from bson.objectid import ObjectId

from PIL import Image, ImageDraw
from io import BytesIO
from base64 import b64decode

import os
import json
import numpy as np
import logging

import utils
import cv2

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info(f"Starting FACE worker...")
# Init Kafka consumer
kafka_consumer = Consumer(
    {
        'bootstrap.servers': Config.KAFKA_BROKER,
        'group.id' : Config.KAFKA_GROUP,
        'auto.offset.reset': 'earliest'
    }
)
logging.info(f"Created consumer.")

# Init MongoDB collection
mongo_client = MongoClient(Config.MONGO_CON_STRING)
mongo_db = mongo_client[Config.MONGO_DB]
mongo_incidents_collection = mongo_db[Config.MONGO_INCIDENTS_COLLECTION]
logging.info(f"MongoDB connection successfull.")

# Init and load Milvus collection
milvus_client = MilvusClient(uri = Config.MILVUS_URI, db_name = Config.MILVUS_DB)
milvus_client.load_collection(Config.MILVUS_COLLECTION)
logging.info(f"Milvus connection successfull.")

# Variables for duplicate checking
incident_id = -1
incident_iin_and_vectors = [] # [[vector_0, iin_0], [vector_1, iin_1], ...] 
frame_save_cntr = 0
logging.info(f"Waiting for messages to come...")

# checking Kafka message for errors
def msg_has_error(msg):
    if msg is None:
        return True
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            logging.info(f'End of offset {msg.topic()} [{msg.partition()}]')
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
    faces = msg.get('faces')
    msg = f"content_id: {content_id}, timestamp: {timestamp}"

    if content_id == None or file_type == None or timestamp == None or frame == None or faces == None:
        logging.info('******************** Incorrect input fields! ********************')
        logging.info(msg)
        return True

    if type(content_id) != str or type(file_type) != str or type(timestamp) != str or type(frame) != str or type(faces) != list:
        logging.info('******************** Incorrect input fields type! ********************')
        logging.info(msg)
        return True

    if file_type != 'video' and file_type != 'image':
        logging.info('******************** Incorrect value of "file_type" field! ********************')
        logging.info(msg)
        return True
    
    # Need check other fields value

    for face in faces:
        vector = face.get('vector')
        coordinates = face.get('coordinates')
        if vector == None or coordinates == None:
            logging.info('******************** Incorrect keys "faces" field! ********************')
            logging.info(msg)
            return True
        if type(vector) != list or type(coordinates) != list:
            logging.info('******************** Incorrect types of "faces" field values! ********************')
            logging.info(msg)
            return True
        if len(vector) != 512:
            logging.info('******************** Incorrect vector dimension! ********************')
            logging.info(msg)
            return True
        
        # Need check 'coordinates' field

    return False

# Checking vector with vectors from incident_iin_and_vectors
# If has duplicate return iin, else return None
def duplicate_checking(content_id, new_vector):
    global incident_id, incident_iin_and_vectors, frame_save_cntr

    if incident_id == content_id:
        for element in incident_iin_and_vectors:
            angle = np.dot(element[0], np.array(new_vector).T)
            logging.info(f'Angle with vectors - {angle}')
            if (angle > 0.5): 
                if element[1] == None: return -1
                return element[1]
        logging.info('---')
    else: 
        incident_id = content_id
        incident_iin_and_vectors.clear()
        frame_save_cntr = 0
    
    incident_iin_and_vectors.append([new_vector, None])
    return None


# get IIN from Milvus DB by vector
def get_iin_by_vector(vector):
    result = milvus_client.search(
        collection_name = Config.MILVUS_COLLECTION,
        data = vector,
        limit = 1,
        search_params = {"metric_type": "COSINE", "params":{}}
    )
    result = result[0][0]
    logging.info(f"distance - {result.get('distance')}")
    
    if result.get("distance") > 0.5: 
        return result.get("id")
    return None 


# Save frame and crops in Local file storage - OUTPUT
def save_crops(content_id, frame, coord_iin, file_type):
    image_bytes = b64decode(frame)
    # frame.decode('base64')
    image = Image.open(BytesIO(image_bytes))

    path = Config.OUTPUT_PATH
    path = os.path.join(path, content_id)
    if not os.path.exists(path): os.mkdir(path)

    global frame_save_cntr
    # video_0.jpg or image_0.jpg
    # crop_0_0.jpg
    # crop_0_1.jpg
    # crop_1_0.jpg
    frame_name = file_type + '_' + str(frame_save_cntr) + '.jpg'
    # if file_type == 'video': image.save(os.path.join(path, frame_name))
    image.save(os.path.join(path, frame_name))
    
    # if file_type == 'video': draw = ImageDraw.Draw(image)

    # Output list - [{'crop': crop_name, 'iin': iin}, ...]
    faces = []
    frame_width, frame_height = image.size
    crop_cntr = 0
    
    for element in coord_iin:
        iin = element['iin']
        box = element['coord']
        # logging.info(f'coord: {box}')
        bboxes = element['bbox']
        # logging.info(f'bbox: {bboxes}')
        x, y, width, height = box

        logging.info(f'frame width - {frame_width}, frame height - {frame_height}')
        # logging.info(f'face coordinates: x - {x}, y - {y}, width - {width}, height - {height}')

        if frame_width < width + x or frame_height < height + y: 
            logging.info('******************** Incorrect face coordinates! ********************')
            logging.info(f'frame width - {frame_width}, frame height - {frame_height}')
            logging.info(f'face coordinates: x - {x}, y - {y}, width - {width}, height - {height}')
            continue

        # if file_type == 'video': draw.rectangle((x, y, width, height), outline='yellow', width=2)
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        crop = utils.return_cropped_face(img, bboxes)
        if crop is not None:
            crop_name = 'crop_' + str(frame_save_cntr) + '_' + str(crop_cntr) + '.jpg'
            # cropped_image = image.crop((x, frame_height - (y + height), x + width, frame_height - y))
            # cropped_image.save(os.path.join(path, crop_name))
            
            # Saving cropped image in a destination directory
            cv2.imwrite(os.path.join(path, crop_name), crop)
            faces.append({'crop': crop_name, 'iin': iin, 'coordinates': box})
            crop_cntr += 1
    
    # if file_type == 'video': image.save(path + frame_name)

    frame_save_cntr += 1

    return frame_name, faces


def saveMongo(content_id, output_list):
    mongo_incidents_collection.update_one({"_id": ObjectId(content_id)},{"$set":{"analytics_result": output_list}})

    logging.info('****************************************************************')
    logging.info('Message saved!')
    logging.info(f'Content_id: {content_id}  \n\n')


# Infinity loop with receiving msg from kafka topic 
# Check duplicate
# Get iin from Milvus by vector
# Save frame and crops in Local file storage - OUTPUT
# Save output in Mongo
def consume_and_save():
    try:
        kafka_consumer.subscribe([Config.KAFKA_FACE_TOPIC])
        i = 0
        output_list = []
        isSaved = False
        while True:
            msg = kafka_consumer.poll(timeout=10)

            if msg_has_error(msg): 
                if i != 0 and not isSaved: 
                    saveMongo(incident_id, output_list)
                    isSaved = True
                continue

            msg = json.loads(msg.value())

            if i != 0 and msg.get('content_id') != incident_id:
                if not isSaved: saveMongo(incident_id, output_list)
                output_list = []
            isSaved = False

            logging.info('****************************************************************')
            logging.info(f'Message received: {i}')
            i+=1
        
            # check field values from message
            if msg_values_is_incorrect(msg): 
                continue

            content_id = msg.get('content_id')
            file_type = msg.get('file_type')
            timestamp = msg.get('timestamp')
            frame = msg.get('frame')
            frame_cnt = msg.get('count_frames')
            faces = msg.get('faces')
            file_name = msg.get('file_name')

            logging.info(f'faces - {len(faces)}')
            
            coord_iin = [] # [{'coord': [], 'iin': 4646}, {'coord': [], 'iin': 1359}, ...]
            new_person_flag = False
        
            for face in faces:
                vector = face.get('vector')
                coordinates = face.get('coordinates')
                bbox = face.get('bbox')

                iin = duplicate_checking(content_id, vector)
                coord_iin.append({'coord': coordinates, 'iin': iin, 'bbox': bbox})
                if iin == -1: coord_iin[-1]['iin'] = None
                if iin is not None:
                    continue

                new_person_flag = True
                
                # Get iin from Milvus db
                iin = get_iin_by_vector([vector])
                
                if iin is not None: 
                    coord_iin[-1]['iin'] = iin
                    incident_iin_and_vectors[-1][1] = iin

            # If there isn't a new person then skip the message
            if new_person_flag: 
                logging.info(f'New Person flag. list elem - {len(incident_iin_and_vectors)}')
                # Save frame, crops and return picture names
                frame_name, faces = save_crops(content_id, frame, coord_iin, file_type)

                # Save dict in Mongo 
                output_data = {
                    'content_id': content_id, 
                    'file_type': file_type, 
                    'file_name': file_name,
                    'frame': frame_name,
                    'faces': faces
                }
                output_list.append(output_data)
            else: 
                logging.info('Duplicate frame!')

            
            
    except KafkaException as e:
        logging.error(f'Error in kafka')
        # logging.error(f"[KafkaException]: {e.strerror}, filename: {e.filename}")
    finally: 
        kafka_consumer.close()
        mongo_client.close()
        milvus_client.release_collection(Config.MILVUS_COLLECTION)
        milvus_client.close()
        

# Init module
if __name__ == '__main__': 
    consume_and_save()

# INPUT

# 'content_id': '641323', 
# 'file_type': 'video', 
# 'timestamp': '2024-07-28 20:56:26.679209',
# 'frame': base64_img_1 
# 'frame_cnt': 15
# 'faces': [
#     {'vector': [], 'coordinates': [x, y, width, height]}, 
#     {'vector': [], 'coordinates': [x, y, width, height]}, 
#     ...
# ]


#OUTPUT

# 'content_id' : 64512
# 'file_type': 'img'
# 'frame': frame_1.jpg
# 'faces': [
#     {
#         'crop': 'crop_1.jpg', 
#         'iin': 654321, 
#         'coordinates': [x, y, width, height]
#     }, 
#     ...
# ]

# Сохранить координаты или сразу кадры с квадратом