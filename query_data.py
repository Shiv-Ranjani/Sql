#!/usr/bin/env python3
"""
Data Query and Validation Script
Checks data quality and provides insights from all databases
"""

import logging
import sys
import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from database_utils import DatabaseManager
from star_schema import StarSchemaManager

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self):
        self.raw_db = DatabaseManager(Config.RAW_DB_URL, Config.RAW_SCHEMA)
        self.processed_db = DatabaseManager(Config.PROCESSED_DB_URL, Config.PROCESSED_SCHEMA)
        self.warehouse = StarSchemaManager(Config.WAREHOUSE_DB_URL, Config.WAREHOUSE_SCHEMA)
        
    def validate_all_databases(self):
        """Validate data in all databases"""
        try:
            logger.info("Starting comprehensive data validation")
            
            results = {
                'timestamp': datetime.now().isoformat(),
                'raw_data': self.validate_raw_data(),
                'processed_data': self.validate_processed_data(),
                'star_schema': self.validate_star_schema(),
                'data_quality': self.generate_data_quality_report()
            }
            
            # Save results
            with open('validation_results.json', 'w') as f:
                json.dump(results, f, indent=2)
            
            logger.info("Validation completed and saved to validation_results.json")
            return results
            
        except Exception as e:
            logger.error(f"Error during validation: {e}")
            return None
    
    def validate_raw_data(self):
        """Validate raw data database"""
        try:
            if not self.raw_db.connect():
                return {'error': 'Failed to connect to raw database'}
            
            # Get table info for MySQL
            tables = self.raw_db.execute_query(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{Config.RAW_SCHEMA}'
            """)
            
            results = {
                'tables': [table[0] for table in tables],
                'raw_data_stats': {}
            }
            
            # Check raw_data table
            if tables and 'raw_data' in [table[0] for table in tables]:
                stats = self.raw_db.execute_query(f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(DISTINCT country) as unique_countries,
                        COUNT(DISTINCT invoice_date) as unique_dates,
                        MIN(invoice_date) as earliest_date,
                        MAX(invoice_date) as latest_date,
                        COUNT(DISTINCT customer_id) as unique_customers,
                        COUNT(DISTINCT stock_code) as unique_products
                    FROM {Config.RAW_SCHEMA}.raw_data
                """)
                
                if stats:
                    results['raw_data_stats'] = {
                        'total_rows': stats[0][0],
                        'unique_countries': stats[0][1],
                        'unique_dates': stats[0][2],
                        'earliest_date': str(stats[0][3]) if stats[0][3] else None,
                        'latest_date': str(stats[0][4]) if stats[0][4] else None,
                        'unique_customers': stats[0][5],
                        'unique_products': stats[0][6]
                    }
            
            self.raw_db.close()
            return results
            
        except Exception as e:
            logger.error(f"Error validating raw data: {e}")
            return {'error': str(e)}
    
    def validate_processed_data(self):
        """Validate processed data database"""
        try:
            if not self.processed_db.connect():
                return {'error': 'Failed to connect to processed database'}
            
            # Get table info for MySQL
            tables = self.processed_db.execute_query(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{Config.PROCESSED_SCHEMA}'
            """)
            
            results = {
                'tables': [table[0] for table in tables],
                'processed_data_stats': {}
            }
            
            # Check processed_data table
            if tables and 'processed_data' in [table[0] for table in tables]:
                stats = self.processed_db.execute_query(f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(CASE WHEN is_valid = 1 THEN 1 END) as valid_rows,
                        COUNT(CASE WHEN is_valid = 0 THEN 1 END) as invalid_rows,
                        COUNT(DISTINCT country) as unique_countries,
                        COUNT(DISTINCT invoice_date) as unique_dates,
                        COUNT(DISTINCT customer_id) as unique_customers,
                        COUNT(DISTINCT stock_code) as unique_products,
                        SUM(total_amount) as total_sales,
                        AVG(unit_price) as avg_unit_price
                    FROM {Config.PROCESSED_SCHEMA}.processed_data
                """)
                
                if stats:
                    results['processed_data_stats'] = {
                        'total_rows': stats[0][0],
                        'valid_rows': stats[0][1],
                        'invalid_rows': stats[0][2],
                        'unique_countries': stats[0][3],
                        'unique_dates': stats[0][4],
                        'unique_customers': stats[0][5],
                        'unique_products': stats[0][6],
                        'total_sales': float(stats[0][7]) if stats[0][7] else 0,
                        'avg_unit_price': float(stats[0][8]) if stats[0][8] else 0,
                        'data_quality_percentage': round((stats[0][1] / stats[0][0]) * 100, 2) if stats[0][0] > 0 else 0
                    }
            
            self.processed_db.close()
            return results
            
        except Exception as e:
            logger.error(f"Error validating processed data: {e}")
            return {'error': str(e)}
    
    def validate_star_schema(self):
        """Validate star schema data warehouse"""
        try:
            if not self.warehouse.connect():
                return {'error': 'Failed to connect to data warehouse'}
            
            # Get star schema statistics
            stats = self.warehouse.get_star_schema_stats()
            
            # Additional queries for insights
            insights = {}
            
            # Top countries by sales
            top_countries = self.warehouse.execute_query(f"""
                SELECT 
                    dc.country_name,
                    COUNT(fs.fact_id) as records,
                    SUM(fs.total_amount) as total_sales,
                    AVG(fs.unit_price) as avg_unit_price
                FROM {Config.WAREHOUSE_SCHEMA}.fact_sales fs
                JOIN {Config.WAREHOUSE_SCHEMA}.dim_country dc ON fs.country_id = dc.country_id
                WHERE fs.is_valid = 1
                GROUP BY dc.country_name
                ORDER BY total_sales DESC
                LIMIT 10
            """)
            
            if top_countries:
                insights['top_countries'] = [
                    {
                        'country': row[0],
                        'records': row[1],
                        'total_sales': float(row[2]) if row[2] else 0,
                        'avg_unit_price': float(row[3]) if row[3] else 0
                    }
                    for row in top_countries
                ]
            
            # Customer segment distribution
            customer_distribution = self.warehouse.execute_query(f"""
                SELECT 
                    dc.customer_segment,
                    COUNT(fs.fact_id) as transaction_count,
                    SUM(fs.total_amount) as total_sales
                FROM {Config.WAREHOUSE_SCHEMA}.fact_sales fs
                JOIN {Config.WAREHOUSE_SCHEMA}.dim_customer dc ON fs.customer_id = dc.customer_id
                WHERE fs.is_valid = 1
                GROUP BY dc.customer_segment
                ORDER BY total_sales DESC
            """)
            
            if customer_distribution:
                insights['customer_distribution'] = [
                    {
                        'customer_segment': row[0],
                        'transaction_count': row[1],
                        'total_sales': float(row[2]) if row[2] else 0
                    }
                    for row in customer_distribution
                ]
            
            # Monthly sales trends
            monthly_trends = self.warehouse.execute_query(f"""
                SELECT 
                    dd.year,
                    dd.month,
                    dd.month_name,
                    COUNT(fs.fact_id) as transaction_count,
                    SUM(fs.total_amount) as total_sales,
                    AVG(fs.unit_price) as avg_unit_price
                FROM {Config.WAREHOUSE_SCHEMA}.fact_sales fs
                JOIN {Config.WAREHOUSE_SCHEMA}.dim_date dd ON fs.date_id = dd.date_id
                WHERE fs.is_valid = 1
                GROUP BY dd.year, dd.month, dd.month_name
                ORDER BY dd.year, dd.month
            """)
            
            if monthly_trends:
                insights['monthly_trends'] = [
                    {
                        'year': row[0],
                        'month': row[1],
                        'month_name': row[2],
                        'transaction_count': row[3],
                        'total_sales': float(row[4]) if row[4] else 0,
                        'avg_unit_price': float(row[5]) if row[5] else 0
                    }
                    for row in monthly_trends
                ]
            
            results = {
                'statistics': stats,
                'insights': insights
            }
            
            self.warehouse.close()
            return results
            
        except Exception as e:
            logger.error(f"Error validating star schema: {e}")
            return {'error': str(e)}
    
    def generate_data_quality_report(self):
        """Generate comprehensive data quality report"""
        try:
            report = {
                'data_completeness': {},
                'data_consistency': {},
                'data_accuracy': {},
                'recommendations': []
            }
            
            # Check data completeness
            if self.processed_db.connect():
                completeness_stats = self.processed_db.execute_query(f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(CASE WHEN country IS NULL THEN 1 END) as missing_country,
                        COUNT(CASE WHEN invoice_date IS NULL THEN 1 END) as missing_date,
                        COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as missing_customer_id,
                        COUNT(CASE WHEN stock_code IS NULL THEN 1 END) as missing_stock_code,
                        COUNT(CASE WHEN total_amount IS NULL THEN 1 END) as missing_total_amount
                    FROM {Config.PROCESSED_SCHEMA}.processed_data
                """)
                
                if completeness_stats:
                    stats = completeness_stats[0]
                    total = stats[0]
                    report['data_completeness'] = {
                        'total_rows': total,
                        'missing_country_pct': round((stats[1] / total) * 100, 2) if total > 0 else 0,
                        'missing_date_pct': round((stats[2] / total) * 100, 2) if total > 0 else 0,
                        'missing_customer_id_pct': round((stats[3] / total) * 100, 2) if total > 0 else 0,
                        'missing_stock_code_pct': round((stats[4] / total) * 100, 2) if total > 0 else 0,
                        'missing_total_amount_pct': round((stats[5] / total) * 100, 2) if total > 0 else 0
                    }
                
                self.processed_db.close()
            
            # Check data consistency
            if self.warehouse.connect():
                consistency_stats = self.warehouse.execute_query(f"""
                    SELECT 
                        COUNT(*) as total_facts,
                        COUNT(CASE WHEN total_amount < 0 THEN 1 END) as negative_amounts,
                        COUNT(CASE WHEN unit_price < 0 THEN 1 END) as negative_prices,
                        COUNT(CASE WHEN quantity < 0 THEN 1 END) as negative_quantities
                    FROM {Config.WAREHOUSE_SCHEMA}.fact_sales
                """)
                
                if consistency_stats:
                    stats = consistency_stats[0]
                    total = stats[0]
                    report['data_consistency'] = {
                        'total_facts': total,
                        'negative_amounts_pct': round((stats[1] / total) * 100, 2) if total > 0 else 0,
                        'negative_prices_pct': round((stats[2] / total) * 100, 2) if total > 0 else 0,
                        'negative_quantities_pct': round((stats[3] / total) * 100, 2) if total > 0 else 0
                    }
                
                self.warehouse.close()
            
            # Generate recommendations
            recommendations = []
            
            if report['data_completeness'].get('missing_customer_id_pct', 0) > 5:
                recommendations.append("High percentage of missing customer data - consider data source validation")
            
            if report['data_consistency'].get('negative_amounts_pct', 0) > 0:
                recommendations.append("Negative amounts found - implement data validation rules")
            
            if report['data_consistency'].get('negative_prices_pct', 0) > 0:
                recommendations.append("Negative prices detected - review data processing logic")
            
            report['recommendations'] = recommendations
            
            # Save report
            with open('data_quality_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating data quality report: {e}")
            return {'error': str(e)}
    
    def run_sample_queries(self):
        """Run sample analytical queries"""
        try:
            logger.info("Running sample analytical queries")
            
            if not self.warehouse.connect():
                logger.error("Failed to connect to data warehouse")
                return
            
            queries = {
                'total_sales_by_country': f"""
                    SELECT 
                        dc.country_name,
                        COUNT(fs.fact_id) as transaction_count,
                        SUM(fs.total_amount) as total_sales,
                        AVG(fs.unit_price) as avg_unit_price
                    FROM {Config.WAREHOUSE_SCHEMA}.fact_sales fs
                    JOIN {Config.WAREHOUSE_SCHEMA}.dim_country dc ON fs.country_id = dc.country_id
                    WHERE fs.is_valid = 1
                    GROUP BY dc.country_name
                    ORDER BY total_sales DESC
                    LIMIT 10
                """,
                
                'sales_trends_by_month': f"""
                    SELECT 
                        dd.year,
                        dd.month_name,
                        COUNT(fs.fact_id) as transaction_count,
                        SUM(fs.total_amount) as total_sales,
                        AVG(fs.unit_price) as avg_unit_price
                    FROM {Config.WAREHOUSE_SCHEMA}.fact_sales fs
                    JOIN {Config.WAREHOUSE_SCHEMA}.dim_date dd ON fs.date_id = dd.date_id
                    WHERE fs.is_valid = 1
                    GROUP BY dd.year, dd.month_name
                    ORDER BY dd.year, dd.month
                """,
                
                'customer_segment_analysis': f"""
                    SELECT 
                        dc.customer_segment,
                        COUNT(DISTINCT dc.customer_id) as unique_customers,
                        COUNT(fs.fact_id) as total_transactions,
                        SUM(fs.total_amount) as total_sales,
                        AVG(fs.total_amount) as avg_transaction_value
                    FROM {Config.WAREHOUSE_SCHEMA}.fact_sales fs
                    JOIN {Config.WAREHOUSE_SCHEMA}.dim_customer dc ON fs.customer_id = dc.customer_id
                    WHERE fs.is_valid = 1
                    GROUP BY dc.customer_segment
                    ORDER BY total_sales DESC
                """,
                
                'top_products_by_sales': f"""
                    SELECT 
                        dp.stock_code,
                        dp.description,
                        COUNT(fs.fact_id) as transaction_count,
                        SUM(fs.total_amount) as total_sales,
                        AVG(fs.unit_price) as avg_unit_price
                    FROM {Config.WAREHOUSE_SCHEMA}.fact_sales fs
                    JOIN {Config.WAREHOUSE_SCHEMA}.dim_product dp ON fs.product_id = dp.product_id
                    WHERE fs.is_valid = 1
                    GROUP BY dp.stock_code, dp.description
                    ORDER BY total_sales DESC
                    LIMIT 10
                """
            }
            
            results = {}
            for query_name, query in queries.items():
                try:
                    result = self.warehouse.execute_query(query)
                    if result:
                        results[query_name] = [
                            {col: val for col, val in zip(range(len(row)), row)}
                            for row in result
                        ]
                except Exception as e:
                    logger.warning(f"Error executing {query_name}: {e}")
                    results[query_name] = {'error': str(e)}
            
            # Save query results
            with open('sample_queries_results.json', 'w') as f:
                json.dump(results, f, indent=2)
            
            logger.info("Sample queries completed and saved to sample_queries_results.json")
            self.warehouse.close()
            
        except Exception as e:
            logger.error(f"Error running sample queries: {e}")

def main():
    """Main function to run data validation"""
    try:
        validator = DataValidator()
        
        # Run comprehensive validation
        validation_results = validator.validate_all_databases()
        
        if validation_results:
            print("✅ Data validation completed successfully!")
            print(f"Results saved to: validation_results.json")
            print(f"Data quality report saved to: data_quality_report.json")
            
            # Run sample queries
            validator.run_sample_queries()
            print(f"Sample queries saved to: sample_queries_results.json")
            
        else:
            print("❌ Data validation failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 