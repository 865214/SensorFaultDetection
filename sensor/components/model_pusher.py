from sensor.exception import CustomException
from sensor.logger import logging
from sensor.entity.artifact_entity import ModelPusherArtifact, ModelEvaluationArtifact
from sensor.entity.config_entity import ModelPusherConfig
import os
import sys
import shutil

class ModelPusher:
    """
    This class is responsible for pushing the trained model to the desired directory locations.
    It copies the model from the evaluation artifact to the specified paths in the configuration.
    """
    
    def __init__(self, model_pusher_config: ModelPusherConfig, model_eval_artifact: ModelEvaluationArtifact):
        """
        Initializes the ModelPusher with configuration and model evaluation artifact.
        
        :param model_pusher_config: ModelPusherConfig object containing the configuration for model pushing.
        :param model_eval_artifact: ModelEvaluationArtifact object containing the evaluated model details.
        """
        try:
            logging.info("Initializing ModelPusher.")
            self.model_pusher_config = model_pusher_config
            self.model_eval_artifact = model_eval_artifact
            logging.info(f"ModelPusherConfig: {self.model_pusher_config}")
            logging.info(f"ModelEvaluationArtifact: {self.model_eval_artifact}")
        except Exception as e:
            logging.error(f"Error during initialization: {e}")
            raise CustomException(e, sys)
        
    def initiate_model_pusher(self) -> ModelPusherArtifact:
        """
        Initiates the model pushing process by copying the trained model to the specified directories.
        
        :return: ModelPusherArtifact object containing the paths of the pushed model.
        """
        try:
            logging.info("Starting model pusher process.")
            
            trained_model_path = self.model_eval_artifact.trained_model_path
            model_file_path = self.model_pusher_config.model_file_path
            saved_model_path = self.model_pusher_config.saved_model_path

            logging.info(f"Trained model path: {trained_model_path}")
            logging.info(f"Model file path: {model_file_path}")
            logging.info(f"Saved model path: {saved_model_path}")

            logging.info("Creating model pusher directory to save model.")
            os.makedirs(os.path.dirname(model_file_path), exist_ok=True)
            shutil.copy(src=trained_model_path, dst=model_file_path)
            logging.info(f"Model copied to model file path: {model_file_path}")

            os.makedirs(os.path.dirname(saved_model_path), exist_ok=True)
            shutil.copy(src=trained_model_path, dst=saved_model_path)
            logging.info(f"Model copied to saved model path: {saved_model_path}")

            logging.info("Preparing ModelPusherArtifact.")
            model_pusher_artifact = ModelPusherArtifact(
                saved_model_path=saved_model_path,
                model_file_path=model_file_path
            )
            logging.info(f"ModelPusherArtifact created: {model_pusher_artifact}")
            return model_pusher_artifact

        except Exception as e:
            raise CustomException(e, sys)
