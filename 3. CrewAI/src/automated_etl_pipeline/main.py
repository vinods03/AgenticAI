#!/usr/bin/env python
import sys
import warnings

from datetime import datetime

from automated_etl_pipeline.crew import AutomatedEtlPipeline

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")

requirements = """
   Build an ETL pipeline using Pyspark that:
1. Extracts sales data from a Kinesis Data Stream with columns: order_id, customer_id, 
   product_id, quantity, price, order_date, region
2. Transforms the data by:
   - Converting dates to standard format
   - Calculating total_amount (quantity * price)
   - Adding derived columns: year, month, quarter
   - Removing duplicates based on order_id
   - Validating that quantity > 0 and price > 0
   - Standardizing region names (uppercase, trim whitespace)
3. Loads the transformed data into a Redshift database table 'sales_fact'. 
4. Provide the DDL for Redshift database table 'sales_fact' in a separate SQL file.
5. Includes data quality checks for:
   - No null values in critical columns
   - Positive quantities and prices
   - Valid date ranges
   - No duplicate order_ids
"""
pipeline_name = 'sales_etl'
source = 'Kinesis Data Stream'
destination = 'Redshift'

def run():
    """
    Run the crew.
    """
    inputs = {
        'requirements': requirements,
        'pipeline_name': pipeline_name,
        'source': source,
        'destination': destination
    }
    
    try:
        AutomatedEtlPipeline().crew().kickoff(inputs=inputs)
    except Exception as e:
        raise Exception(f"An error occurred while running the crew: {e}")

