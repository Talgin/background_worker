from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from kafka import KafkaConsumer
import json
import asyncio
import asyncpg
import os
import logging
from pymilvus import MilvusClient

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL database setup
DATABASE_URL = os.getenv('PG_DATABASE_URL')

async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    logger.info("Connected to the database")
    table_exists = await conn.fetchval('''
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'innout'
        );
    ''')
    if not table_exists:
        await conn.execute('''
            CREATE TABLE public.innout (
            id serial4 NOT NULL,
            point_id int4 NOT NULL,
            card_id varchar NOT NULL,
            decision bool NOT NULL,
            crop_url varchar NOT NULL,
            original_image_url varchar NOT NULL,
            time_of_action timestamp NOT NULL,
            "gender" varchar NOT NULL,
            age int4 NOT NULL,
            camera_id int4 NOT NULL,
            CONSTRAINT innout_pkey PRIMARY KEY (id)
            );
        ''')
        logger.info("Table created")
    else:
        logger.info("Table already exists")
    await conn.close()

# Kafka consumer setup
KAFKA_INPUT_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_CONSUMER_TOPIC = os.getenv('KAFKA_CONSUMER_TOPIC')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP')

try:
    consumer = KafkaConsumer(
        KAFKA_CONSUMER_TOPIC,
        bootstrap_servers=[KAFKA_INPUT_SERVER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info("Connected to Kafka")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")

# Init and load Milvus collection
milvus_client = MilvusClient(uri = os.getenv('MILVUS_URI'), db_name = os.getenv('MILVUS_DB'))
milvus_client.load_collection(os.getenv('MILVUS_COLLECTION'))
logging.info(f"Milvus connection successfull.")

# SSE setup
clients = ['']

@app.get("/events")
async def get_events(request: Request):
    async def event_generator():
        conn = await asyncpg.connect(DATABASE_URL)
        logger.info("Connected to the database for event streaming")
        while True:
            if await request.is_disconnected():
                clients.remove(request)
                break
            if clients:
                try:
                    for message in consumer:
                        data = message.value
                        logging.info('Data received from Kafka: %s', data)
                        # Get IIN from Milvus DB

                        # End of Get IIN from Milvus DB

                        # Save to database
                        try:
                            # await conn.execute('''
                            # INSERT INTO innout (point_id, card_id, decision, crop_url, original_image_url, time_of_action, gender, age, camera_id)
                            # VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            # ''', data['point_id'], data['card_id'], data['decision'], data['crop_url'], data['original_image_url'], data['time_of_action'], data['gender'], data['age'], data['camera_id'])
                            await conn.execute('''
                            INSERT INTO innout (point_id, card_id, decision, crop_url, original_image_url, time_of_action, gender, age, camera_id)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ''', None, data['card_id'], None, data['crop_path'], data['frame_path'], data['time_of_action'], None, None, data['camera_id'])
                            # {'start_time': start_time, 'crop_path': aligned_path, 'frame_path': original_frame_path, 'time_of_action': timestamp, 'gender': None, 'age': None, 'camera_id': camera_id, 'face_embedding': embs, 'face_info': res_dict, 'camera_name': camera_name}
                            logger.info(f"Data written to database: {data}")
                        except Exception as e:
                            logger.error(f"Failed to write data to database: {e}")
                        
                        # Send to clients
                        for client in clients:
                            yield json.dumps(data)
                            # await client.put(json.dumps(data))
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
            await asyncio.sleep(1)
        await conn.close()

    clients.append(request)
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.on_event("startup")
async def startup_event():
    global clients
    clients = []
    await init_db()

@app.on_event("shutdown")
async def shutdown_event():
    pass

# get IIN from Milvus DB by vector
def get_iin_by_vector(vector):
    result = milvus_client.search(
        collection_name = os.getenv('MILVUS_COLLECTION'),
        data = vector,
        limit = 1,
        search_params = {"metric_type": "COSINE", "params":{}}
    )
    result = result[0][0]
    logging.info(f"distance - {result.get('distance')}")
    
    if result.get("distance") > 0.5: 
        return result.get("id")
    return None

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=40003)