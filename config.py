import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # MySQL database configuration - single database with 3 schemas
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
    MYSQL_USER = os.getenv('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'ecommerce_data_pipeline')
    
    # Schema names for the three data layers
    RAW_SCHEMA = os.getenv('RAW_SCHEMA', 'raw_data')
    PROCESSED_SCHEMA = os.getenv('PROCESSED_SCHEMA', 'processed_data')
    WAREHOUSE_SCHEMA = os.getenv('WAREHOUSE_SCHEMA', 'data_warehouse')
    
    # Database URLs for each schema
    RAW_DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    PROCESSED_DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    WAREHOUSE_DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

    # Kaggle configuration - updated to ecommerce dataset
    KAGGLE_DATASET = os.getenv('KAGGLE_DATASET', 'carrie1/ecommerce-data')
    KAGGLE_FILE = os.getenv('KAGGLE_FILE', 'ecommerce_data.csv')

    # Data processing settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '5000'))

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO') 