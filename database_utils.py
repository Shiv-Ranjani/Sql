import logging
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Date, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from config import Config

# Configure logging
logging.basicConfig(level=getattr(logging, Config.LOG_LEVEL))
logger = logging.getLogger(__name__)

Base = declarative_base()

class DatabaseManager:
    def __init__(self, db_url, schema_name=None):
        self.db_url = db_url
        self.schema_name = schema_name
        self.engine = None
        self.Session = None
        
    def connect(self):
        """Create database connection"""
        try:
            self.engine = create_engine(self.db_url)
            self.Session = sessionmaker(bind=self.engine)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Successfully connected to database: {self.db_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_schema(self):
        """Create schema if it doesn't exist"""
        if self.schema_name:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"))
                    conn.commit()
                logger.info(f"Schema {self.schema_name} created/verified successfully")
            except Exception as e:
                logger.error(f"Failed to create schema {self.schema_name}: {e}")
    
    def create_tables(self):
        """Create all tables in the specified schema"""
        try:
            if self.schema_name:
                # Set the schema for all tables
                for table in Base.metadata.tables.values():
                    table.schema = self.schema_name
            Base.metadata.create_all(self.engine)
            logger.info(f"Tables created successfully in schema: {self.schema_name}")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
    
    def insert_dataframe(self, df, table_name, if_exists='replace'):
        """Insert DataFrame to database table"""
        try:
            if self.schema_name:
                full_table_name = f"{self.schema_name}.{table_name}"
            else:
                full_table_name = table_name
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, method='multi', schema=self.schema_name)
            logger.info(f"Successfully inserted {len(df)} rows into {full_table_name}")
        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {e}")
    
    def execute_query(self, query):
        """Execute a SQL query and return results"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return result.fetchall()
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return None
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")

# Raw data table definition for ecommerce dataset
class RawData(Base):
    __tablename__ = 'raw_data'
    
    id = Column(Integer, primary_key=True)
    invoice_no = Column(String(50))
    stock_code = Column(String(50))
    description = Column(Text)
    quantity = Column(Integer)
    invoice_date = Column(DateTime)
    unit_price = Column(Float)
    customer_id = Column(String(50))
    country = Column(String(100))

# Processed data table definition for ecommerce dataset
class ProcessedData(Base):
    __tablename__ = 'processed_data'
    
    id = Column(Integer, primary_key=True)
    invoice_no = Column(String(50))
    stock_code = Column(String(50))
    description = Column(Text)
    quantity = Column(Integer)
    invoice_date = Column(DateTime)
    unit_price = Column(Float)
    customer_id = Column(String(50))
    country = Column(String(100))
    total_amount = Column(Float)
    is_valid = Column(Integer)  # 1 for valid, 0 for invalid
    created_at = Column(DateTime) 