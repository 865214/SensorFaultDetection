from typing import Optional
import numpy as np
import pandas as pd
import json
from sensor.configuration.mongo_db_connection import MongoDBCLient
from sensor.constant.database import DATABASE_NAME
from sensor.exception import CustomException
from sensor.logger import logging
import sys,os


class SensorData:
    """"
    This class helps us to export entire MongoDB record as a pandas DataFrame
    """
    
    def __init__(self):
        try:
            logging.info(f"Connecting to MongoDB database: {DATABASE_NAME}")
            self.mongo_client = MongoDBCLient(database_name=DATABASE_NAME)
            logging.info("Successfully connected to MongoDB.")
        except Exception as e:
            logging.error(f"Error connecting to MongoDB: {e}")
            raise CustomException(e, sys)
        
    def save_csv_file(self, file_path, collection_name: str, database_name: Optional[str] = None):
        try:
            data_frame = pd.read_csv(file_path)
            data_frame.reset_index(drop=True, inplace=True)
            records = list(json.loads(data_frame.T.to_json()).values())
            
            dir_path = os.path.dirname(file_path)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
                
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]
            collection.insert_many(records)
            return len(records)
        except Exception as e:
            raise CustomException(e, sys)
        
    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        try:
            logging.info(f"Exporting collection '{collection_name}' from database '{database_name or DATABASE_NAME}' to DataFrame.")
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]
            
            logging.info(f"Fetching documents from collection '{collection_name}'.")
            documents = list(collection.find())
            logging.info(f"Number of documents fetched: {len(documents)}")
            
            if not documents:
                logging.error(f"No documents found in collection '{collection_name}'")
                raise ValueError(f"No documents found in collection '{collection_name}'")
            
            df = pd.DataFrame(documents)
            
            if df.empty:
                logging.error("The DataFrame is empty after exporting data from MongoDB.")
                raise ValueError("The exported DataFrame is empty. Check the data in MongoDB.")
            
            if "_id" in df.columns.to_list():  # Correct usage
                df = df.drop(columns=["_id"], axis=1)
            
            df.replace({"na": np.nan}, inplace=True)
            logging.info(f"DataFrame exported successfully with shape: {df.shape}")
            
            return df
        except Exception as e:
            logging.error(f"Error exporting collection as DataFrame: {e}")
            raise CustomException(e, sys)