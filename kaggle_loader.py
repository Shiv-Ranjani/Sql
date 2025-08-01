import os
import logging
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from config import Config

logger = logging.getLogger(__name__)

class KaggleLoader:
    def __init__(self):
        self.api = None
        self.kaggle_json_path = 'kaggle.json'
        
    def authenticate(self):
        """Authenticate with Kaggle API using kaggle.json file"""
        try:
            # Check if kaggle.json exists
            if not os.path.exists(self.kaggle_json_path):
                logger.error(f"kaggle.json file not found at {self.kaggle_json_path}")
                return False
            
            # Set environment variable for Kaggle credentials
            os.environ['KAGGLE_CONFIG_DIR'] = os.getcwd()
            
            # Initialize Kaggle API
            self.api = KaggleApi()
            self.api.authenticate()
            
            logger.info("Successfully authenticated with Kaggle API")
            return True
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Kaggle API: {e}")
            return False
    
    def download_dataset(self, dataset_name, file_name=None):
        """Download dataset from Kaggle"""
        try:
            if not self.api:
                if not self.authenticate():
                    return None
            
            # Create data directory if it doesn't exist
            os.makedirs('data', exist_ok=True)
            
            # Download the dataset
            logger.info(f"Downloading dataset: {dataset_name}")
            self.api.dataset_download_files(
                dataset_name,
                path='data',
                unzip=True
            )
            
            # If specific file is requested, return its path
            if file_name:
                file_path = os.path.join('data', file_name)
                if os.path.exists(file_path):
                    logger.info(f"Dataset downloaded successfully: {file_path}")
                    return file_path
                else:
                    logger.error(f"File {file_name} not found in downloaded dataset")
                    return None
            
            # Return the data directory path
            logger.info("Dataset downloaded successfully to data/ directory")
            return 'data'
            
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            return None
    
    def load_data(self, dataset_name=None, file_name=None):
        """Load data from Kaggle and return as DataFrame"""
        try:
            # Use default dataset if none specified
            if not dataset_name:
                dataset_name = Config.KAGGLE_DATASET
            
            if not file_name:
                file_name = Config.KAGGLE_FILE
            
            # Download dataset
            file_path = self.download_dataset(dataset_name, file_name)
            
            if not file_path:
                return None
            
            # Load data into DataFrame
            logger.info(f"Loading data from {file_path}")
            df = pd.read_csv(file_path)
            
            logger.info(f"Successfully loaded {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return None
    
    def get_dataset_info(self, dataset_name):
        """Get information about a dataset"""
        try:
            if not self.api:
                if not self.authenticate():
                    return None
            
            dataset_info = self.api.dataset_view(dataset_name)
            return dataset_info
            
        except Exception as e:
            logger.error(f"Failed to get dataset info: {e}")
            return None 