import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def initialize_glue():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    return sc, glueContext, spark, job

def read_dynamodb(glueContext, dynamodb_table):
    logger.info(f"Reading from DynamoDB table: {dynamodb_table}")
    return glueContext.create_dynamic_frame_from_options(
        connection_type="dynamodb",
        connection_options={
            "dynamodb.input.tableName": dynamodb_table
            ,
            "dynamodb.throughput.read.percent": "0.5"
        }
    )

def clean_and_transform_data(dyf):
    df = dyf.toDF()
    
    # Filter completed trips
    df = df.filter(F.col('status') == 'completed')
    
    # Check required fields
    required_columns = ['trip_id', 'pickup_datetime', 'dropoff_datetime', 'fare']
    for col_name in required_columns:
        df = df.filter(F.col(col_name).isNotNull())

    # Cast fare
    df = df.withColumn('fare_amount', F.col('fare').cast(DoubleType()))
    
    # Extract trip_date from pickup_datetime and convert to date type
    df = df.withColumn('trip_date', F.to_date('pickup_datetime'))
    
    return df

def calculate_kpis(df):
    kpi_df = df.groupBy('trip_date').agg(
        F.sum('fare_amount').alias('total_fare'),
        F.count('trip_id').alias('count_trips'),
        F.avg('fare_amount').alias('average_fare'),
        F.max('fare_amount').alias('max_fare'),
        F.min('fare_amount').alias('min_fare')
    )
    return kpi_df

def write_to_s3(kpi_df, output_s3_path):
    logger.info(f"Writing KPIs to S3: {output_s3_path}")
    kpi_df.write.mode('overwrite').partitionBy('trip_date').json(output_s3_path)

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DYNAMO_TABLE', 'OUTPUT_S3_PATH'])
    dynamodb_table = args['DYNAMO_TABLE']
    output_s3_path = args['OUTPUT_S3_PATH']

    sc, glueContext, spark, job = initialize_glue()
    
    try:
        dyf = read_dynamodb(glueContext, dynamodb_table)
        if dyf.count() == 0:
            logger.info("No data found in DynamoDB table. Exiting gracefully.")
            job.commit()
            sys.exit(0)
        
        df_cleaned = clean_and_transform_data(dyf)
        kpi_df = calculate_kpis(df_cleaned)
        write_to_s3(kpi_df, output_s3_path)
    
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        raise e

    job.commit()

if __name__ == "__main__":
    main()
