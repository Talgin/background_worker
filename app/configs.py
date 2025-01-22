from urllib.parse import quote_plus

class Config:
    OUTPUT_PATH = '/STORAGE/output/analytics_face'
    OUTPUT_PATH_LPR = '/STORAGE/output/analytics_car'

    #Mongo configuration
    MONGO_SERVER = '192.168.1.85:27021'
    MONGO_DB = 'law_abiding_citizen'
    MONGO_USER = 'law_abiding_citizen'
    MONGO_PASS = 'law_abiding_citizen'
    MONGO_LPR_COLLECTION = 'analytics_lpr_collection'
    MONGO_INCIDENTS_COLLECTION = 'incidents'

    MONGO_CON_STRING = "mongodb://%s:%s@%s" % (quote_plus(MONGO_USER), quote_plus(MONGO_PASS), MONGO_SERVER)


    #Kafka configuration
    KAFKA_BROKER = '192.168.1.85:40002'
    KAFKA_GROUP = 'my-group'
    KAFKA_LPR_TOPIC = 'alpr_results'
    KAFKA_FACE_TOPIC = 'face_results'


    #Milvus configuration
    MILVUS_URI = 'http://192.168.1.85:19530'
    MILVUS_DB = 'test'
    MILVUS_COLLECTION = 'people'

    # Face attributes
    min_head_size = 20
    crop_extender = 28
