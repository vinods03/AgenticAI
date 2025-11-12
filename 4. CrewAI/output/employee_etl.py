from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("employee_etl") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
logger = glueContext.get_logger()

# Configuration for Kinesis and Redshift connection
kinesisStreamName = "your_kinesis_stream_name"
redshiftTempS3Dir = "s3://your-temp-bucket/temp/"
redshiftJdbcUrl = "jdbc:redshift://your-redshift-cluster:5439/your-database"
redshiftUser = "your-username"
redshiftPassword = "your-password"
redshiftTable = "your-schema.your-table"

# Read transformation mapping rules
transformation_df = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .load("C:/Users/VINOD/projects/agents/3_crew/enhanced_automated_etl_pipeline/employee_transformation.xlsx")

transformations = transformation_df.collect()

# Function to apply transformations
def apply_transformations(df, transformations):
    for row in transformations:
        source_col = row['Source Column']
        target_col = row['Target Column']
        rule = row['Transformation Rule']
        df = df.withColumn(target_col, expr(rule.format(source_col)))
    return df

# Extract data from Kinesis
raw_df = spark.readStream \
    .format("kinesis") \
    .option("streamName", kinesisStreamName) \
    .option("region", "us-west-2") \
    .load()

# Transform data
transformed_df = apply_transformations(raw_df, transformations)

# Function to handle errors
def handle_errors(batch_df, batch_id):
    error_df = batch_df.filter("errorColumn IS NOT NULL")
    if error_df.count() > 0:
        error_df.write.mode("append").json("s3://your-error-bucket/errors/")

# Load data to Redshift
def foreach_batch_function(batch_df, batch_id):
    dynamic_frame = DynamicFrame.fromDF(batch_df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame = dynamic_frame,
        catalog_connection = "redshift_connection",
        connection_options = {
            "preactions": "truncate table {}".format(redshiftTable),
            "dbtable": redshiftTable,
            "database": "your-database"
        },
        redshift_tmp_dir = redshiftTempS3Dir
    )
    handle_errors(batch_df, batch_id)
    
# Define streaming query
query = transformed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .start()

# Await termination
query.awaitTermination()