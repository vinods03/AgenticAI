```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, quarter, trim, upper, date_format
from pyspark.sql.types import IntegerType, DecimalType, DateType
import boto3
import redshift_connector
from pyspark.streaming.kinesis import KinesisUtils
from functools import reduce

def create_spark_session():
    spark = SparkSession.builder \
        .appName('sales_etl') \
        .getOrCreate()
    return spark

def extract_from_kinesis(KINESIS_STREAM_NAME, KINESIS_REGION_NAME):
    kinesis_client = boto3.client('kinesis', region_name=KINESIS_REGION_NAME)
    kinesis_stream_arn = kinesis_client.describe_stream(StreamName=KINESIS_STREAM_NAME)['StreamDescription']['StreamARN']
    
    # Assuming the use of Spark Structured Streaming to read from Kinesis Data Stream
    spark = create_spark_session()
    raw_df = spark.readStream \
        .format("kinesis") \
        .option("streamName", KINESIS_STREAM_NAME) \
        .option("initialPosition", "earliest") \
        .option("region", KINESIS_REGION_NAME) \
        .option("awsAccessKeyId", "YOUR_ACCESS_KEY_ID") \
        .option("awsSecretKey", "YOUR_SECRET_ACCESS_KEY") \
        .load()
    return raw_df

def transform_data(df_raw):
    df_transformed = df_raw.withColumn("order_date", date_format(col("order_date"), "yyyy-MM-dd")) \
                           .withColumn("total_amount", col("quantity") * col("price")) \
                           .withColumn("year", year(col("order_date"))) \
                           .withColumn("month", month(col("order_date"))) \
                           .withColumn("quarter", quarter(col("order_date"))) \
                           .dropDuplicates(["order_id"]) \
                           .filter((col("quantity") > 0) & (col("price") > 0)) \
                           .withColumn("region", upper(trim(col("region"))))
    
    # Data Quality Checks
    data_quality_checks = {
        "null_check": df_transformed.filter(reduce(lambda a, b: a | b, [col(c).isNull() for c in df_transformed.columns])).count() == 0,
        "positive_values": df_transformed.filter((col("quantity") > 0) & (col("price") > 0)).count() == df_transformed.count()
    }
    
    assert all(data_quality_checks.values()), "Data Quality Checks Failed"
    
    return df_transformed

def load_to_redshift(df_transformed, redshift_jdbc_url, redshift_user, redshift_password):
    # Redshift connection
    conn = redshift_connector.connect(
        host=redshift_jdbc_url,
        database='your_database',
        user=redshift_user,
        password=redshift_password
    )
    # Writing data to Redshift
    df_transformed.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", redshift_jdbc_url) \
        .option("tempdir", "s3a://your-temp-bucket/temp-dir/") \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", "sales_fact") \
        .option("user", redshift_user) \
        .option("password", redshift_password) \
        .mode("append") \
        .save()

def run_etl_pipeline():
    # Define variables
    KINESIS_STREAM_NAME = 'your_kinesis_stream_name'
    KINESIS_REGION_NAME = 'your_region'
    redshift_jdbc_url = 'jdbc:redshift://your-redshift-endpoint:5439/yourdb'
    redshift_user = 'your_user'
    redshift_password = 'your_password'

    try:
        df_raw = extract_from_kinesis(KINESIS_STREAM_NAME, KINESIS_REGION_NAME)
        df_transformed = transform_data(df_raw)
        load_to_redshift(df_transformed, redshift_jdbc_url, redshift_user, redshift_password)
        print("ETL Pipeline executed successfully!")
    except Exception as e:
        print(f"Error in ETL Process: {str(e)}")
        # Implement retry logic or alert via AWS SNS
        raise

run_etl_pipeline()
```