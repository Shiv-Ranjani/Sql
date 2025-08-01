import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Date, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import Config

logger = logging.getLogger(__name__)

Base = declarative_base()

# Star Schema Table Definitions for Ecommerce Data
class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    
    customer_id = Column(String(50), primary_key=True)
    customer_segment = Column(String(20))  # Low, Medium, High
    country = Column(String(100))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class DimDate(Base):
    __tablename__ = 'dim_date'
    
    date_id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    quarter = Column(Integer)
    day_of_week = Column(Integer)
    day_name = Column(String(20))
    month_name = Column(String(20))
    is_weekend = Column(Integer)  # 1 for weekend, 0 for weekday
    created_at = Column(DateTime, default=datetime.now)

class DimProduct(Base):
    __tablename__ = 'dim_product'
    
    product_id = Column(String(50), primary_key=True)
    stock_code = Column(String(50))
    description = Column(Text)
    product_category = Column(String(50))
    created_at = Column(DateTime, default=datetime.now)

class DimCountry(Base):
    __tablename__ = 'dim_country'
    
    country_id = Column(Integer, primary_key=True)
    country_name = Column(String(100), nullable=False)
    region = Column(String(50))  # Can be derived or added later
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class FactSales(Base):
    __tablename__ = 'fact_sales'
    
    fact_id = Column(Integer, primary_key=True)
    customer_id = Column(String(50), ForeignKey('dim_customer.customer_id'))
    date_id = Column(Integer, ForeignKey('dim_date.date_id'))
    product_id = Column(String(50), ForeignKey('dim_product.product_id'))
    country_id = Column(Integer, ForeignKey('dim_country.country_id'))
    
    # Fact measures
    quantity = Column(Integer)
    unit_price = Column(Float)
    total_amount = Column(Float)
    invoice_no = Column(String(50))
    
    # Derived measures
    rolling_7d_sales = Column(Float)
    
    # Metadata
    is_valid = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)

class StarSchemaManager:
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
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Successfully connected to data warehouse: {self.db_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to data warehouse: {e}")
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
        """Create all star schema tables in the specified schema"""
        try:
            if self.schema_name:
                # Set the schema for all tables
                for table in Base.metadata.tables.values():
                    table.schema = self.schema_name
            Base.metadata.create_all(self.engine)
            logger.info(f"Star schema tables created successfully in schema: {self.schema_name}")
        except Exception as e:
            logger.error(f"Failed to create star schema tables: {e}")
    
    def populate_dimensions(self, df):
        """Populate dimension tables from processed ecommerce data"""
        try:
            session = self.Session()
            
            # Populate DimCustomer
            logger.info("Populating DimCustomer dimension")
            customers = df[['customer_id', 'customer_segment', 'country']].drop_duplicates()
            for _, row in customers.iterrows():
                if pd.notna(row['customer_id']):
                    customer = DimCustomer(
                        customer_id=row['customer_id'],
                        customer_segment=row.get('customer_segment', 'Unknown'),
                        country=row.get('country', 'Unknown')
                    )
                    session.add(customer)
            session.commit()
            
            # Populate DimDate
            logger.info("Populating DimDate dimension")
            dates = df['invoice_date'].drop_duplicates()
            for date in dates:
                if pd.notna(date):
                    date_obj = pd.to_datetime(date)
                    dim_date = DimDate(
                        date=date_obj.date(),
                        year=date_obj.year,
                        month=date_obj.month,
                        day=date_obj.day,
                        quarter=date_obj.quarter,
                        day_of_week=date_obj.dayofweek,
                        day_name=date_obj.strftime('%A'),
                        month_name=date_obj.strftime('%B'),
                        is_weekend=1 if date_obj.dayofweek >= 5 else 0
                    )
                    session.add(dim_date)
            session.commit()
            
            # Populate DimProduct
            logger.info("Populating DimProduct dimension")
            products = df[['stock_code', 'description', 'product_category']].drop_duplicates()
            for _, row in products.iterrows():
                if pd.notna(row['stock_code']):
                    product = DimProduct(
                        product_id=row['stock_code'],
                        stock_code=row['stock_code'],
                        description=row.get('description', ''),
                        product_category=row.get('product_category', 'OTHER')
                    )
                    session.add(product)
            session.commit()
            
            # Populate DimCountry
            logger.info("Populating DimCountry dimension")
            countries = df['country'].drop_duplicates()
            for country in countries:
                if pd.notna(country):
                    dim_country = DimCountry(
                        country_name=country,
                        region=self._categorize_region(country)
                    )
                    session.add(dim_country)
            session.commit()
            
            session.close()
            logger.info("Dimension tables populated successfully")
            
        except Exception as e:
            logger.error(f"Error populating dimensions: {e}")
            session.rollback()
            session.close()
    
    def populate_facts(self, df):
        """Populate fact table from processed ecommerce data"""
        try:
            session = self.Session()
            
            logger.info("Populating FactSales fact table")
            
            # Get dimension mappings
            customer_map = {row.customer_id: row.customer_id for row in session.query(DimCustomer).all()}
            date_map = {row.date: row.date_id for row in session.query(DimDate).all()}
            product_map = {row.product_id: row.product_id for row in session.query(DimProduct).all()}
            country_map = {row.country_name: row.country_id for row in session.query(DimCountry).all()}
            
            # Process data in batches
            batch_size = 1000
            total_rows = len(df)
            
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    try:
                        # Get dimension keys
                        customer_id = customer_map.get(row['customer_id'])
                        date_id = date_map.get(pd.to_datetime(row['invoice_date']).date())
                        product_id = product_map.get(row['stock_code'])
                        country_id = country_map.get(row['country'])
                        
                        if customer_id and date_id and product_id and country_id:
                            fact = FactSales(
                                customer_id=customer_id,
                                date_id=date_id,
                                product_id=product_id,
                                country_id=country_id,
                                quantity=row.get('quantity'),
                                unit_price=row.get('unit_price'),
                                total_amount=row.get('total_amount'),
                                invoice_no=row.get('invoice_no'),
                                rolling_7d_sales=row.get('rolling_7d_sales'),
                                is_valid=row.get('is_valid', 1)
                            )
                            session.add(fact)
                    
                    except Exception as e:
                        logger.warning(f"Error processing row: {e}")
                        continue
                
                session.commit()
                logger.info(f"Processed batch {i//batch_size + 1}/{(total_rows + batch_size - 1)//batch_size}")
            
            session.close()
            logger.info("Fact table populated successfully")
            
        except Exception as e:
            logger.error(f"Error populating facts: {e}")
            session.rollback()
            session.close()
    
    def _categorize_region(self, country_name):
        """Categorize countries into regions"""
        if pd.isna(country_name):
            return 'Unknown'
        
        country_lower = country_name.lower()
        
        # European countries
        european_countries = ['united kingdom', 'germany', 'france', 'italy', 'spain', 'netherlands', 
                            'belgium', 'switzerland', 'austria', 'sweden', 'norway', 'denmark', 'finland']
        if any(country in country_lower for country in european_countries):
            return 'Europe'
        
        # North American countries
        north_american_countries = ['united states', 'canada', 'mexico']
        if any(country in country_lower for country in north_american_countries):
            return 'North America'
        
        # Asian countries
        asian_countries = ['japan', 'china', 'india', 'singapore', 'south korea', 'thailand', 'malaysia']
        if any(country in country_lower for country in asian_countries):
            return 'Asia'
        
        # Australian/Oceanian countries
        australian_countries = ['australia', 'new zealand']
        if any(country in country_lower for country in australian_countries):
            return 'Oceania'
        
        return 'Other'
    
    def get_star_schema_stats(self):
        """Get statistics about the star schema"""
        try:
            session = self.Session()
            
            stats = {
                'dim_customer_count': session.query(DimCustomer).count(),
                'dim_date_count': session.query(DimDate).count(),
                'dim_product_count': session.query(DimProduct).count(),
                'dim_country_count': session.query(DimCountry).count(),
                'fact_sales_count': session.query(FactSales).count(),
                'valid_facts_count': session.query(FactSales).filter(FactSales.is_valid == 1).count(),
                'invalid_facts_count': session.query(FactSales).filter(FactSales.is_valid == 0).count()
            }
            
            session.close()
            return stats
            
        except Exception as e:
            logger.error(f"Error getting star schema stats: {e}")
            return None
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Data warehouse connection closed") 