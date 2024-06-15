import os, sys
from sensor.constant.training_pipeline import SAVED_MODEL_DIR, MODEL_FILE_NAME
from sensor.exception import CustomException
class TargetValueMapping:
    def __init__(self):
        self.neg: int = 0
        self.pos: int = 1
    def to_dict(self):
        return self.__dict__
    
    def reverse_mapping(self):
        mapping_response = self.to_dict
        return dict(zip(mapping_response.values(), mapping_response.keys()))
    
    
class SensorModel:
    def __init__(self, preprocessor, model):
        try:
            self.preprocessor = preprocessor
            self.model = model
        except Exception as e:
            raise e
        
class ModelResolver:
    def __init__(self, model_dir=SAVED_MODEL_DIR):
        try:
            self.model_dir = model_dir
            if not os.path.exists(self.model_dir):
                os.makedirs(self.model_dir, exist_ok=True)
        except Exception as e:
            raise CustomException(e, sys)

    def get_best_model_path(self) -> str:
        try:
            timestamps = [int(ts) for ts in os.listdir(self.model_dir)]
            if not timestamps:
                raise CustomException(e, sys)
            
            latest_timestamp = max(timestamps)
            latest_model_path = os.path.join(self.model_dir, str(latest_timestamp), MODEL_FILE_NAME)
            return latest_model_path
        except Exception as e:
            raise CustomException(e, sys)

    def is_model_exists(self) -> bool:
        try:
            if not os.path.exists(self.model_dir):
                return False
            
            timestamps = os.listdir(self.model_dir)
            if not timestamps:
                return False
            
            latest_model_path = self.get_best_model_path()
            
            return os.path.exists(latest_model_path)
        except Exception as e:
            raise CustomException(e, sys)