# Ecommerce Data Pipeline with Star Schema

A comprehensive Python data pipeline that fetches ecommerce data from Kaggle, processes it, and implements a star schema data warehouse using MySQL.

## Features

- **Kaggle Integration**: Fetches ecommerce datasets using Kaggle API with `kaggle.json` credentials
- **MySQL Single Database Architecture**: Stores data in three schemas within a single MySQL database:
  - `raw_data` schema for raw data
  - `processed_data` schema for cleaned data
  - `data_warehouse` schema for star schema
- **Data Processing**: Comprehensive data cleaning, validation, and transformation
- **Star Schema**: Implements dimensional modeling with fact and dimension tables
- **GitHub Actions**: Automated pipeline execution with CI/CD
- **Data Validation**: Comprehensive data quality checks and reporting

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Kaggle API    │───▶│   Raw Schema     │───▶│ Processed Schema│
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Data Validation │◀───│  Star Schema     │◀───│ Data Warehouse  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Star Schema Design

### Dimension Tables
- **DimCustomer**: Customer information (customer_id, customer_segment, country)
- **DimDate**: Date dimensions (date_id, date, year, month, day, quarter, etc.)
- **DimProduct**: Product information (product_id, stock_code, description, product_category)
- **DimCountry**: Country information (country_id, country_name, region)

### Fact Table
- **FactSales**: Sales metrics and measures
  - Foreign keys to all dimension tables
  - Measures: quantity, unit_price, total_amount, invoice_no
  - Derived measures: rolling_7d_sales

## Setup Instructions

### Prerequisites

1. **Python 3.9+**
2. **MySQL Database**
3. **Kaggle API Credentials**

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Kaggle credentials**
   - Download your `kaggle.json` from Kaggle API settings
   - Place it in the project root directory

4. **Configure environment variables**
   Create a `.env` file:
   ```env
   MYSQL_HOST=localhost
   MYSQL_PORT=3306
   MYSQL_USER=root
   MYSQL_PASSWORD=password
   MYSQL_DATABASE=ecommerce_data_pipeline
   RAW_SCHEMA=raw_data
   PROCESSED_SCHEMA=processed_data
   WAREHOUSE_SCHEMA=data_warehouse
   KAGGLE_DATASET=carrie1/ecommerce-data
   KAGGLE_FILE=ecommerce_data.csv
   BATCH_SIZE=1000
   CHUNK_SIZE=5000
   LOG_LEVEL=INFO
   ```

5. **Create MySQL database and schemas**
   ```sql
   CREATE DATABASE ecommerce_data_pipeline;
   USE ecommerce_data_pipeline;
   CREATE SCHEMA raw_data;
   CREATE SCHEMA processed_data;
   CREATE SCHEMA data_warehouse;
   ```

## Usage

### Running the Pipeline

1. **Execute the main pipeline**
   ```bash
   python main_pipeline.py
   ```

2. **Validate data**
   ```bash
   python query_data.py
   ```

### GitHub Actions Setup

1. **Add repository secrets**:
   - `KAGGLE_USERNAME`: Your Kaggle username
   - `KAGGLE_KEY`: Your Kaggle API key

2. **The workflow will automatically**:
   - Set up MySQL database with schemas
   - Install dependencies
   - Run the data pipeline
   - Execute data validation
   - Upload artifacts (logs, reports)

## File Structure

```
├── main_pipeline.py          # Main pipeline orchestration
├── config.py                 # Configuration management
├── kaggle_loader.py          # Kaggle API integration
├── database_utils.py         # Database utilities
├── data_processor.py         # Data cleaning and processing
├── star_schema.py           # Star schema implementation
├── query_data.py            # Data validation and queries
├── requirements.txt          # Python dependencies
├── .github/workflows/       # GitHub Actions workflows
│   └── data-pipeline.yml
├── README.md                # This file
└── kaggle.json             # Kaggle API credentials (not in repo)
```

## Data Processing Steps

1. **Data Extraction**: Fetches ecommerce data from Kaggle using API
2. **Raw Storage**: Stores original data in raw_data schema
3. **Data Cleaning**: 
   - Handles missing values
   - Removes duplicates
   - Validates data types
   - Identifies outliers
4. **Data Transformation**:
   - Creates derived features (total_amount, customer_segments)
   - Extracts date components
   - Calculates rolling averages
   - Categorizes products and regions
5. **Processed Storage**: Stores cleaned data in processed_data schema
6. **Star Schema Population**:
   - Creates dimension tables (customers, dates, products, countries)
   - Populates fact table (sales)
   - Establishes relationships

## Data Quality Features

- **Completeness Checks**: Missing value analysis for customer_id, stock_code, etc.
- **Consistency Validation**: Data type and range checks for amounts and prices
- **Accuracy Metrics**: Outlier detection and validation
- **Quality Reports**: Automated data quality assessment

## Sample Queries

The validation script includes sample analytical queries:

```sql
-- Top countries by sales
SELECT dc.country_name, SUM(fs.total_amount) as total_sales
FROM fact_sales fs
JOIN dim_country dc ON fs.country_id = dc.country_id
WHERE fs.is_valid = 1
GROUP BY dc.country_name
ORDER BY total_sales DESC;

-- Customer segment analysis
SELECT dc.customer_segment, COUNT(fs.fact_id) as transaction_count
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
WHERE fs.is_valid = 1
GROUP BY dc.customer_segment;

-- Monthly sales trends
SELECT dd.year, dd.month_name, SUM(fs.total_amount) as monthly_sales
FROM fact_sales fs
JOIN dim_date dd ON fs.date_id = dd.date_id
WHERE fs.is_valid = 1
GROUP BY dd.year, dd.month_name;
```

## Output Files

- `pipeline.log`: Detailed execution logs
- `pipeline_summary.json`: Pipeline execution summary
- `validation_results.json`: Data validation results
- `data_quality_report.json`: Comprehensive data quality report
- `sample_queries_results.json`: Sample analytical query results

## Monitoring and Logging

- Comprehensive logging throughout the pipeline
- Error handling and recovery mechanisms
- Performance metrics and timing information
- Data quality metrics and validation reports

## Troubleshooting

### Common Issues

1. **Kaggle Authentication Error**
   - Ensure `kaggle.json` is in the project root
   - Verify API credentials are correct

2. **MySQL Connection Error**
   - Check MySQL connection parameters in configuration
   - Ensure MySQL is running
   - Verify database and schema permissions

3. **Memory Issues**
   - Adjust `BATCH_SIZE` and `CHUNK_SIZE` in config
   - Process data in smaller chunks

### Logs

Check the following files for debugging:
- `pipeline.log`: Main execution logs
- `validation_results.json`: Data validation details
- `data_quality_report.json`: Data quality issues

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs and validation reports
3. Open an issue on GitHub 