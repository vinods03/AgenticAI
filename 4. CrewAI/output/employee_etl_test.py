Here is a comprehensive Python file containing Pytest unit test cases for the Pyspark ETL pipeline. Ensure to place it in the same directory as your PySpark project or indicate proper paths when importing modules.

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import os

# Initialize PySpark Session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("employee_etl_test") \
        .master("local[*]") \
        .getOrCreate()

def test_schema_validation(spark, actual_data):
    """Validate that the dataframe has the correct schema."""
    expected_schema = StructType([
        StructField("EmployeeID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Salary", DoubleType(), True),
        StructField("JoinDate", DateType(), True)
    ])
    assert actual_data.schema == expected_schema, "Schema does not match"

def test_data_type_validation(spark, actual_data):
    """Validate data types of the columns."""
    expected_dtypes = {
        "EmployeeID": IntegerType(),
        "Name": StringType(),
        "Department": StringType(),
        "Salary": DoubleType(),
        "JoinDate": DateType()
    }
    for field in actual_data.schema.fields:
        assert type(field.dataType) == type(expected_dtypes[field.name]), f"{field.name} has an incorrect data type"

def test_non_null_critical_fields(spark, actual_data):
    """Check that critical fields are non-null."""
    non_nullable_columns = ["EmployeeID", "Name", "JoinDate"]
    for column in non_nullable_columns:
        assert actual_data.filter(col(column).isNull()).count() == 0, f"{column} contains null values"

def test_value_range(validation, actual_data):
    """Check that Salary is within a reasonable range"""
    min_salary = 30000  # Up to your business logic
    max_salary = 200000  # Up to your business logic
    assert actual_data.filter((col("Salary") < min_salary) | (col("Salary") > max_salary)).count() == 0, "Salaries are out of expected range"

def test_duplicate_detection(spark, actual_data):
    """Detect duplicate records based on EmployeeID."""
    count_before = actual_data.count()
    count_after = actual_data.dropDuplicates(subset=["EmployeeID"]).count()
    assert count_before == count_after, "Duplicate EmployeeID found"

def test_referential_integrity(spark, actual_data, department_data):
    """Check that Department in employees exist in the department table."""
    department_ids = [row['DepartmentID'] for row in department_data.select("DepartmentID").collect()]
    invalid_departments = actual_data.filter(~col("Department").isin(department_ids))
    assert invalid_departments.count() == 0, "Referential integrity check failed for Department"

# Data Profiling - Example
def data_profiling(spark, actual_data):
    """Output basic statistics about the dataset"""
    actual_data.describe().show()

# Provide example or mock dataframe for tests
def create_mock_dataframe(spark):
    """Create a mock dataframe to simulate real data input."""
    data = [
        (1, "Alice", "HR", 50000, "2021-01-15"),
        (2, "Bob", "Engineering", 150000, "2020-06-20"),
        (3, "Charlie", "HR", 70000, "2019-03-30")
    ]
    schema = StructType([
        StructField("EmployeeID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Salary", DoubleType(), True),
        StructField("JoinDate", DateType(), True)
    ])
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="module")
def actual_data(spark):
    return create_mock_dataframe(spark)

# To execute test cases, run `pytest` command in your terminal.
