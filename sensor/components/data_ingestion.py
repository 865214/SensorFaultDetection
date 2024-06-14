import os
import sys
from sensor.exception import CustomException
from sensor.logger import logging
from pandas import DataFrame
from sensor.entity.config_entity import DataIngestionConfig
from sensor.entity.artifact_entity import DataIngestionArtifact
from sensor.data_access.sensor_data import SensorData
from sklearn.model_selection import train_test_split
from sensor.utils.main_utils import read_yaml_file
from sensor.constant.training_pipeline import SCHEMA_FILE_PATH

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
            logging.info(f"Data Ingestion Config: {self.data_ingestion_config.__dict__}")
            logging.info(f"Schema Config: {self._schema_config}")
        except Exception as e:
            raise CustomException(e, sys)
        
    def export_data_into_feature_store(self) -> DataFrame:
        try:
            logging.info("Exporting data from MongoDB to feature store.")
            sensor_data = SensorData()
            logging.info(f"Using collection name: {self.data_ingestion_config.collection_name}")
            dataframe = sensor_data.export_collection_as_dataframe(collection_name=self.data_ingestion_config.collection_name)
            if dataframe.empty:
                logging.error("The DataFrame is empty after exporting data from MongoDB.")
                raise ValueError("The exported DataFrame is empty. Check the data in MongoDB.")
            feature_store_file_path = self.data_ingestion_config.feature_store_dir
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            logging.info(f"Data exported to {feature_store_file_path}")
            return dataframe
        except Exception as e:
            logging.error(f"Error exporting data: {e}")
            raise CustomException(e, sys)
    def split_data_as_train_test(self, dataframe: DataFrame) -> None:
        try:
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logging.info("Performed train-test split on the DataFrame.")
            
            dir_path = os.path.dirname(self.data_ingestion_config.train_file_path)
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"Creating directories: {dir_path}")
            
            train_set.to_csv(self.data_ingestion_config.train_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.test_file_path, index=False, header=True)
            logging.info(f"Train data exported to {self.data_ingestion_config.train_file_path}")
            logging.info(f"Test data exported to {self.data_ingestion_config.test_file_path}")
        except Exception as e:
            logging.error(f"Error splitting data: {e}")
            raise CustomException(e, sys)
    
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            dataframe = self.export_data_into_feature_store()
            logging.info(f"Dataframe shape after export: {dataframe.shape}")
            
            if dataframe.empty:
                logging.error("The DataFrame is empty after exporting data from MongoDB.")
                raise ValueError("The exported DataFrame is empty. Check the data in MongoDB.")
            
            drop_columns = self._schema_config.get('drop_columns', [])
            columns_to_drop = [col for col in drop_columns if col in dataframe.columns]
            if columns_to_drop:
                dataframe = dataframe.drop(self._schema_config['drop_columns'], axis=1)
                logging.info(f"Dropped Column: {columns_to_drop}")
            else:
                logging.warning(f"No columns to drop. Columns requested for dropping: {drop_columns} not found in DataFrame.")
                
            logging.info(f"Dataframe shape after dropping columns: {dataframe.shape}")
            
            self.split_data_as_train_test(dataframe=dataframe)
            data_ingestion_artifact = DataIngestionArtifact(
                train_file_path=self.data_ingestion_config.train_file_path,
                test_file_path=self.data_ingestion_config.test_file_path
            )
            logging.info(f"Data ingestion artifact created: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            logging.error(f"Error initiating data ingestion: {e}")
            raise CustomException(e, sys)