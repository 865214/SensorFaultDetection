from dotenv import load_dotenv
import pymongo
from sensor.constant.database import DATABASE_NAME
import certifi
ca = certifi.where()
from sensor.constant.env_variable import MONGODB_URL_KEY
import os
from sensor.logger import logging
from sensor.exception import CustomException
import sys
load_dotenv()

class MongoDBCLient:
    client = None
    def __init__(self, database_name = DATABASE_NAME)-> None:
        try:
            if MongoDBCLient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                logging.info(f"Retrieved MongoDb URL: {mongo_db_url}")
                
                if "localhost" in mongo_db_url:
                    MongoDBCLient.client = pymongo.MongoClient(mongo_db_url)
                else:
                    MongoDBCLient.client = pymongo.MongoClient(mongo_db_url,tlsCAFile = ca)
                    
            self.client = MongoDBCLient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            
        except Exception as e:
            raise CustomException(e, sys)