# ETL Pipeline Design Document for sales_etl

### Overview
This document outlines the architecture for a PySpark ETL pipeline named `sales_etl` which extracts data from an Amazon Kinesis Data Stream, processes it, and loads it into an Amazon Redshift database table `sales_fact`. The pipeline is designed to be efficient, scalable, and maintainable.

## 1. Data Flow Diagram
```plaintext
[Kinesis Data Stream] --> [PySpark Extract & Transform] --> [Amazon Redshift]
```

## 2. Extraction Strategy
### Connection Details
- **Source**: Amazon Kinesis Data Stream
- **Data Schema**: order_id, customer_id, product_id, quantity, price, order_date, region

### Extraction Strategy
Use the `boto3` library to integrate with AWS SDK, and use `pyspark.streaming.kinesis.KinesisUtils` for streaming data from Kinesis. Kinesis Data Analytics for PySpark can be employed for initial processing.

## 3. Loading Strategy
### Connection Details
- **Destination**: Amazon Redshift
- **Connection Method**: Use the `redshift_connector` library with JDBC for connecting PySpark to Amazon Redshift.

### Loading Strategy
Batch the data and write transformed data to Redshift using DataFrame's `.write()` method configured for Redshift. Use the ‘Copy’ command for efficient bulk loading.

## 4. Transformation Logic
### Business Rules
- **Date Transformation**: Convert `order_date` to `YYYY-MM-DD` format.
- **Calculations**: Add `total_amount` column as `quantity * price`.
- **Derived Columns**: Extract and add `year`, `month`, `quarter` from `order_date`.
- **Deduplication**: Remove duplicate entries based on `order_id`.
- **Validation**: Ensure `quantity` > 0 and `price` > 0.
- **Standardization**: Convert `region` names to uppercase and trim whitespace.

```python
from pyspark.sql.functions import col, year, month, quarter, trim, upper

# Transformation Logic Example
df_transformed = df_raw.withColumn("order_date", date_format(col("order_date"), "yyyy-MM-dd")) \
                       .withColumn("total_amount", col("quantity") * col("price")) \
                       .withColumn("year", year(col("order_date"))) \
                       .withColumn("month", month(col("order_date"))) \
                       .withColumn("quarter", quarter(col("order_date"))) \
                       .dropDuplicates(["order_id"]) \
                       .filter((col("quantity") > 0) & (col("price") > 0)) \
                       .withColumn("region", upper(trim(col("region"))))
```

## 5. Error Handling Approach
Implement try-except blocks to log source or transformation errors, and a retries mechanism. Utilize AWS CloudWatch for log aggregation.

## 6. Validation Rules and Data Quality Checks
- **No Null Values**: Ensure no nulls in `order_id`, `customer_id`, `product_id`, `quantity`, `price`, `order_date`, `region`.
- **Positive Values**: Assert `quantity` > 0 and `price` > 0.
- **Valid Date Range**: Check `order_date` against expected business date bounds.
- **No Duplicates**: Re-validate no `order_id` duplicates after transformation.

```python
from pyspark.sql.functions import isnan, when, count, col

# Data Quality Check Example
data_quality_checks = {
    "null_check": df_transformed.filter(reduce(lambda a, b: a | b, [col(c).isNull() for c in df_transformed.columns])).count() == 0,
    "positive_values": df_transformed.filter((col("quantity") > 0) & (col("price") > 0)).count() == df_transformed.count()
}

assert all(data_quality_checks.values()), "Data Quality Checks Failed"
```

## 7. Logging and Monitoring Strategy
Utilize AWS CloudWatch for monitoring logs and establish alerts for failure thresholds. Incorporate logging to capture metrics: records processed, errors encountered, timestamp of data flow events.

## 8. Performance Considerations
- Partition data by `order_date` for parallel processing.
- Utilize efficient data structures and caching in PySpark to reduce shuffle operations.
- Optimize Redshift table write performance by configuring the correct distribution style and sort keys.

## Redshift Table DDL
```sql
-- sales_fact DDL SQL Script
CREATE TABLE sales_fact (
  order_id VARCHAR(50) PRIMARY KEY,
  customer_id VARCHAR(50) NOT NULL,
  product_id VARCHAR(50) NOT NULL,
  quantity INT NOT NULL CHECK (quantity > 0),
  price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
  total_amount DECIMAL(12, 2) NOT NULL,
  order_date DATE NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  quarter INT NOT NULL,
  region VARCHAR(100) NOT NULL
);
```

This design document outlines an efficient, scalable ETL pipeline using PySpark to extract, transform, and load sales data from Kinesis Streams to Redshift with robust validation, monitoring, and performance strategies.