```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import col

# Initialize Spark Session for Testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("unit-tests") \
        .getOrCreate()

# Define the schema for validation.
expected_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DecimalType(10, 2), False),
    StructField("total_amount", DecimalType(12, 2), False),
    StructField("order_date", DateType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("quarter", IntegerType(), False),
    StructField("region", StringType(), False)
])

# Test Functions

def test_schema_validation(spark, transformed_data):
    assert transformed_data.schema == expected_schema, "Schema does not match the expected schema."

def test_data_type_validation(transformed_data):
    for field in expected_schema.fields:
        assert transformed_data.schema[field.name].dataType == field.dataType, f"Data type of {field.name} does not match."

def test_non_null_critical_fields(transformed_data):
    critical_fields = ["order_id", "customer_id", "product_id", "quantity", "price", "order_date", "region"]
    for field in critical_fields:
        assert transformed_data.filter(col(field).isNull()).count() == 0, f"Null values found in critical field {field}."

def test_positive_quantity_and_price(transformed_data):
    assert transformed_data.filter(col("quantity") <= 0).count() == 0, "Non-positive values found in quantity."
    assert transformed_data.filter(col("price") <= 0).count() == 0, "Non-positive values found in price."

def test_valid_date_range(transformed_data, min_date, max_date):
    assert transformed_data.filter((col("order_date") < min_date) | (col("order_date") > max_date)).count() == 0, "Dates out of valid range found."

def test_no_duplicate_order_ids(transformed_data):
    order_id_count = transformed_data.groupBy("order_id").count()
    assert order_id_count.filter(col("count") > 1).count() == 0, "Duplicate order_id detected."

# Example Data Profiling

def test_data_profiling(actual_data):
    summary_stats = actual_data.describe().collect()
    for row in summary_stats:
        print(f"{row['summary']}: quantity={row['quantity']}, price={row['price']}, total_amount={row['total_amount']}")

# Add more tests as needed for comprehensive coverage...

# Usage example:
# transformed_data = your_transformation_function(actual_raw_dataframe)
# test_schema_validation(spark, transformed_data)
# test_non_null_critical_fields(transformed_data)
# test_positive_quantity_and_price(transformed_data)
# etc.
```

This script includes comprehensive tests for the given ETL pipeline's data quality. Tests are written using Pytest and Pyspark, ensuring the provided data adheres to expected schema, data types, null checks, range validation, and duplicate detection. Each function is designed to accept and operate on dataframes, allowing flexibility for the testing team to input actual data during testing. The use of fixtures to set up a Spark session ensures that the tests are reproducible and isolated. Additional tests or profiling can be added as needed to further ensure data integrity and quality.