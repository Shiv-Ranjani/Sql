#!/usr/bin/env python3
"""
Main Data Pipeline Script
Fetches data from Kaggle, processes it, and implements star schema
"""

import logging
import sys
import os
from datetime import datetime
import pandas as pd

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from kaggle_loader import KaggleLoader
from database_utils import DatabaseManager
from data_processor import DataProcessor
from star_schema import StarSchemaManager

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self):
        self.kaggle_loader = KaggleLoader()
        self.data_processor = DataProcessor()
        self.raw_db = DatabaseManager(Config.RAW_DB_URL, Config.RAW_SCHEMA)
        self.processed_db = DatabaseManager(Config.PROCESSED_DB_URL, Config.PROCESSED_SCHEMA)
        self.warehouse = StarSchemaManager(Config.WAREHOUSE_DB_URL, Config.WAREHOUSE_SCHEMA)
        
    def run_pipeline(self):
        """Execute the complete data pipeline"""
        try:
            logger.info("Starting data pipeline execution")
            start_time = datetime.now()
            
            # Step 1: Load data from Kaggle
            logger.info("Step 1: Loading data from Kaggle")
            raw_data = self.load_kaggle_data()
            if raw_data is None:
                logger.error("Failed to load data from Kaggle")
                return False
            
            # Step 2: Store raw data in database
            logger.info("Step 2: Storing raw data in database")
            if not self.store_raw_data(raw_data):
                logger.error("Failed to store raw data")
                return False
            
            # Step 3: Process and clean data
            logger.info("Step 3: Processing and cleaning data")
            processed_data = self.process_data(raw_data)
            if processed_data is None:
                logger.error("Failed to process data")
                return False
            
            # Step 4: Store processed data in database
            logger.info("Step 4: Storing processed data in database")
            if not self.store_processed_data(processed_data):
                logger.error("Failed to store processed data")
                return False
            
            # Step 5: Create star schema and populate data warehouse
            logger.info("Step 5: Creating star schema and populating data warehouse")
            if not self.create_star_schema(processed_data):
                logger.error("Failed to create star schema")
                return False
            
            # Step 6: Generate pipeline summary
            logger.info("Step 6: Generating pipeline summary")
            self.generate_summary()
            
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"Pipeline completed successfully in {duration}")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {e}")
            return False
    
    def load_kaggle_data(self):
        """Load data from Kaggle"""
        try:
            logger.info("Authenticating with Kaggle API")
            if not self.kaggle_loader.authenticate():
                return None
            
            logger.info("Loading dataset from Kaggle")
            data = self.kaggle_loader.load_data()
            
            if data is not None:
                logger.info(f"Successfully loaded {len(data)} rows and {len(data.columns)} columns")
                return data
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error loading Kaggle data: {e}")
            return None
    
    def store_raw_data(self, data):
        """Store raw data in database"""
        try:
            if not self.raw_db.connect():
                return False
            
            self.raw_db.create_schema()
            self.raw_db.create_tables()
            self.raw_db.insert_dataframe(data, 'raw_data')
            self.raw_db.close()
            
            logger.info("Raw data stored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error storing raw data: {e}")
            return False
    
    def process_data(self, raw_data):
        """Process and clean the raw data"""
        try:
            # Validate raw data
            validation_results = self.data_processor.validate_data(raw_data)
            logger.info(f"Raw data validation: {validation_results}")
            
            # Clean data
            cleaned_data = self.data_processor.clean_data(raw_data)
            if cleaned_data is None:
                return None
            
            # Transform data
            processed_data = self.data_processor.transform_data(cleaned_data)
            if processed_data is None:
                return None
            
            # Generate summary
            summary = self.data_processor.get_data_summary(processed_data)
            logger.info(f"Processed data summary: {summary}")
            
            logger.info("Data processing completed successfully")
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return None
    
    def store_processed_data(self, processed_data):
        """Store processed data in database"""
        try:
            if not self.processed_db.connect():
                return False
            
            self.processed_db.create_schema()
            self.processed_db.create_tables()
            self.processed_db.insert_dataframe(processed_data, 'processed_data')
            self.processed_db.close()
            
            logger.info("Processed data stored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error storing processed data: {e}")
            return False
    
    def create_star_schema(self, processed_data):
        """Create star schema and populate data warehouse"""
        try:
            if not self.warehouse.connect():
                return False
            
            # Create schema and tables
            self.warehouse.create_schema()
            self.warehouse.create_tables()
            
            # Populate dimensions
            self.warehouse.populate_dimensions(processed_data)
            
            # Populate facts
            self.warehouse.populate_facts(processed_data)
            
            # Get statistics
            stats = self.warehouse.get_star_schema_stats()
            logger.info(f"Star schema statistics: {stats}")
            
            self.warehouse.close()
            
            logger.info("Star schema created and populated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating star schema: {e}")
            return False
    
    def generate_summary(self):
        """Generate pipeline execution summary"""
        try:
            summary = {
                'pipeline_status': 'Completed',
                'timestamp': datetime.now().isoformat(),
                'config': {
                    'kaggle_dataset': Config.KAGGLE_DATASET,
                    'kaggle_file': Config.KAGGLE_FILE,
                    'batch_size': Config.BATCH_SIZE,
                    'chunk_size': Config.CHUNK_SIZE
                },
                'database': {
                    'mysql_host': Config.MYSQL_HOST,
                    'mysql_port': Config.MYSQL_PORT,
                    'mysql_database': Config.MYSQL_DATABASE,
                    'raw_schema': Config.RAW_SCHEMA,
                    'processed_schema': Config.PROCESSED_SCHEMA,
                    'warehouse_schema': Config.WAREHOUSE_SCHEMA
                }
            }
            
            # Save summary to file
            import json
            with open('pipeline_summary.json', 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info("Pipeline summary generated and saved to pipeline_summary.json")
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")

def main():
    """Main function to run the pipeline"""
    try:
        pipeline = DataPipeline()
        success = pipeline.run_pipeline()
        
        if success:
            logger.info("Pipeline completed successfully!")
            sys.exit(0)
        else:
            logger.error("Pipeline failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 