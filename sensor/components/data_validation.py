from distutils import dir_util
from sensor.constant.training_pipeline import SCHEMA_FILE_PATH
from sensor.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from sensor.entity.config_entity import DataValidationConfig
from sensor.exception import CustomException
from sensor.logger import logging
from sensor.utils.main_utils import read_yaml_file, write_yaml_file
from scipy.stats import ks_2samp
import pandas as pd
import os
import sys

class DataValidation:

    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise CustomException(e, sys)

    def drop_zero_std_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        try:
            columns_to_drop = [column for column in dataframe.columns if dataframe[column].std() == 0]
            logging.info(f"Dropping columns with zero standard deviation: {columns_to_drop}")
            dataframe.drop(columns=columns_to_drop, inplace=True)
            return dataframe
        except Exception as e:
            raise CustomException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self._schema_config["columns"])
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Data frame has columns: {len(dataframe.columns)}")

            return len(dataframe.columns) == number_of_columns
        except Exception as e:
            raise CustomException(e, sys)

    def is_numerical_column_exist(self, dataframe: pd.DataFrame) -> bool:
        try:
            numerical_columns = self._schema_config["numerical_columns"]
            dataframe_columns = dataframe.columns

            missing_numerical_columns = [num_column for num_column in numerical_columns if num_column not in dataframe_columns]

            if missing_numerical_columns:
                logging.info(f"Missing numerical columns: {missing_numerical_columns}")
                return False

            return True
        except Exception as e:
            raise CustomException(e, sys)

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise CustomException(e, sys)

    def detect_dataset_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, threshold=0.05) -> bool:
        try:
            status = True
            report = {}
            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                is_same_dist = ks_2samp(d1, d2)
                is_found = threshold > is_same_dist.pvalue
                if is_found:
                    status = False
                report.update({
                    column: {
                        "p_value": float(is_same_dist.pvalue),
                        "drift_status": is_found
                    }
                })

            drift_report_file_path = self.data_validation_config.drift_report_file_path

            # Create directory
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)
            return status
        except Exception as e:
            raise CustomException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            error_message = ""
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # Reading data from train and test file location
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            # Validate number of columns
            if not self.validate_number_of_columns(train_dataframe):
                error_message += "Train dataframe does not contain all columns.\n"
            if not self.validate_number_of_columns(test_dataframe):
                error_message += "Test dataframe does not contain all columns.\n"

            # Validate numerical columns
            if not self.is_numerical_column_exist(train_dataframe):
                error_message += "Train dataframe does not contain all numerical columns.\n"
            if not self.is_numerical_column_exist(test_dataframe):
                error_message += "Test dataframe does not contain all numerical columns.\n"

            if error_message:
                raise Exception(error_message)

            logging.info("Checking data drift status")
            status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)

            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.train_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            logging.info(f"Data validation artifact: {data_validation_artifact}")

            return data_validation_artifact
        except Exception as e:
            logging.error(f"Error in initiate_data_validation: {e}")
            raise CustomException(e, sys)