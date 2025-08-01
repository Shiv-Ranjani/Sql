import logging
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from config import Config

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.scaler = StandardScaler()
        
    def clean_data(self, df):
        """Clean and preprocess the raw ecommerce data"""
        try:
            logger.info("Starting data cleaning process")
            
            # Create a copy to avoid modifying original data
            df_clean = df.copy()
            
            # Convert invoice_date column to datetime
            if 'invoice_date' in df_clean.columns:
                df_clean['invoice_date'] = pd.to_datetime(df_clean['invoice_date'], errors='coerce')
            
            # Handle missing values
            numeric_columns = df_clean.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                # Fill missing values with median for numeric columns
                df_clean[col] = df_clean[col].fillna(df_clean[col].median())
            
            # Fill missing values in categorical columns
            categorical_columns = df_clean.select_dtypes(include=['object']).columns
            for col in categorical_columns:
                df_clean[col] = df_clean[col].fillna('Unknown')
            
            # Remove duplicates
            initial_rows = len(df_clean)
            df_clean = df_clean.drop_duplicates()
            logger.info(f"Removed {initial_rows - len(df_clean)} duplicate rows")
            
            # Add data quality indicators
            df_clean['is_valid'] = 1
            
            # Mark rows with extreme outliers as invalid
            for col in numeric_columns:
                if col != 'is_valid':
                    Q1 = df_clean[col].quantile(0.25)
                    Q3 = df_clean[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    # Mark outliers as invalid
                    outlier_mask = (df_clean[col] < lower_bound) | (df_clean[col] > upper_bound)
                    df_clean.loc[outlier_mask, 'is_valid'] = 0
            
            # Add timestamp
            df_clean['created_at'] = datetime.now()
            
            logger.info(f"Data cleaning completed. Final dataset: {len(df_clean)} rows")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error during data cleaning: {e}")
            return None
    
    def validate_data(self, df):
        """Validate data quality"""
        try:
            validation_results = {
                'total_rows': len(df),
                'missing_values': df.isnull().sum().to_dict(),
                'duplicates': df.duplicated().sum(),
                'data_types': df.dtypes.to_dict(),
                'numeric_columns': df.select_dtypes(include=[np.number]).columns.tolist(),
                'categorical_columns': df.select_dtypes(include=['object']).columns.tolist()
            }
            
            logger.info("Data validation completed")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error during data validation: {e}")
            return None
    
    def transform_data(self, df):
        """Apply additional transformations to the ecommerce data"""
        try:
            logger.info("Starting data transformation")
            
            df_transformed = df.copy()
            
            # Create derived features for ecommerce data
            if 'quantity' in df_transformed.columns and 'unit_price' in df_transformed.columns:
                df_transformed['total_amount'] = df_transformed['quantity'] * df_transformed['unit_price']
            
            # Extract date components
            if 'invoice_date' in df_transformed.columns:
                df_transformed['invoice_year'] = df_transformed['invoice_date'].dt.year
                df_transformed['invoice_month'] = df_transformed['invoice_date'].dt.month
                df_transformed['invoice_day'] = df_transformed['invoice_date'].dt.day
                df_transformed['invoice_day_of_week'] = df_transformed['invoice_date'].dt.dayofweek
                df_transformed['invoice_quarter'] = df_transformed['invoice_date'].dt.quarter
            
            # Create customer segments based on total spending
            if 'total_amount' in df_transformed.columns:
                customer_totals = df_transformed.groupby('customer_id')['total_amount'].sum()
                customer_segments = pd.cut(customer_totals, 
                                         bins=[0, customer_totals.quantile(0.33), 
                                               customer_totals.quantile(0.67), float('inf')],
                                         labels=['Low', 'Medium', 'High'])
                df_transformed['customer_segment'] = df_transformed['customer_id'].map(customer_segments)
            
            # Create product categories based on description patterns
            if 'description' in df_transformed.columns:
                df_transformed['product_category'] = df_transformed['description'].str.extract(r'([A-Z]{2,})')[0]
                df_transformed['product_category'] = df_transformed['product_category'].fillna('OTHER')
            
            # Calculate rolling averages for sales metrics
            if 'total_amount' in df_transformed.columns and 'invoice_date' in df_transformed.columns:
                df_transformed = df_transformed.sort_values('invoice_date')
                df_transformed['rolling_7d_sales'] = (
                    df_transformed.groupby('country')['total_amount']
                    .rolling(window=7, min_periods=1)
                    .mean()
                    .reset_index(0, drop=True)
                )
            
            logger.info("Data transformation completed")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error during data transformation: {e}")
            return None
    
    def get_data_summary(self, df):
        """Generate data summary statistics"""
        try:
            summary = {
                'shape': df.shape,
                'columns': df.columns.tolist(),
                'data_types': df.dtypes.to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'numeric_summary': df.describe().to_dict(),
                'categorical_summary': {}
            }
            
            # Add categorical column summaries
            categorical_columns = df.select_dtypes(include=['object']).columns
            for col in categorical_columns:
                summary['categorical_summary'][col] = df[col].value_counts().to_dict()
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating data summary: {e}")
            return None 