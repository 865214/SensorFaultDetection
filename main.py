from sensor.exception import CustomException
from sensor.logger import logging
from sensor.utils2 import dump_csv_file_to_mongodb_collection
from sensor.pipeline.training_pipeline import TrainPipeline
from sensor.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from sensor.entity.config_entity import DataValidationConfig
from sensor.components.data_validation import DataValidation


if __name__ == "__main__":
    train_pipeline = TrainPipeline()
    train_pipeline.run_pipeline()