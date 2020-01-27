import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])


def run_spark_job(spark):

    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "com.udacity.police.calls") \
    .option("maxOffsetsPerTrigger", 150) \
    .load()
        

    # Show schema for the incoming resources for checks
    df.printSchema()
        
    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    
    kafka_df = df.selectExpr("CAST(value AS STRING)")
    
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    service_table.printSchema()
    
    # Select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(psf.to_timestamp(psf.col('call_date_time')).alias('call_date_time'), 
                psf.col('original_crime_type_name'),
                psf.col('disposition'))
    
    distinct_table.printSchema()
    
    # Count the number of original crime type by window and disposition.
    agg_df = distinct_table \
        .withWatermark("call_date_time", "20 minutes") \
        .groupBy(psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
                 "original_crime_type_name",
                 "disposition") \
        .count()
        
    # Get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

   
    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")


    # Join the data streams to get disposition description and
    # Write output stream
    query = agg_df.join(radio_code_df, "disposition")\
    .writeStream\
    .outputMode("complete")\
    .trigger(processingTime="10 seconds") \
    .format("console") \
    .start()

    # Attach a ProgressReporter
    query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.sql.shuffle.partitions", "11") \
        .config("spark.default.parallelism", "11000")\
        .config("spark.streaming.kafka.maxRatePerPartition", "11") \
        .getOrCreate()
    
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()


    #We've made hypertunning over the parameters 
    #("spark.sql.shuffle.partitions",  "spark.streaming.kafka.maxRatePerPartition", "spark.default.parallelism")
    # to get different processedRows & InputRows per second and answer questions.
    
    # Some combinations of tuples we've tested are:
    
    # (25, 25, 2500)
    # "inputRowsPerSecond" : 13.265782281181256,
    #"processedRowsPerSecond" : 85.66508824795523
    
    # (12, 12, 1200)
    #"inputRowsPerSecond" : 14.223922808184994,
    #"processedRowsPerSecond" : 70.80745341614907
    
    # (12, 12, 12000)
    #"inputRowsPerSecond" : 12.225213100044863,
    #"processedRowsPerSecond" : 47.74419623302672
    
    
    # (11, 11, 11000)
    #"inputRowsPerSecond" : 14.9,
    #"processedRowsPerSecond" : 113.740458015267178
    
    # From this combinations, we've checked that the better is the last one so the code reflect this parameters.
    