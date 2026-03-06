# Author: Yoo Xin Wei
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class StreamingProcessor:
    def __init__(self, app_name="AirQualityStreaming"):
        """Initialize Spark Session for Structured Streaming"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
    def read_from_hdfs_stream(self, hdfs_path):
        """Read streaming data from HDFS (processed data from Task 2)"""
        schema = StructType([
            StructField("Country", StringType(), True),
            StructField("State", StringType(), True),
            StructField("City", StringType(), True),
            StructField("AQI_Value", IntegerType(), True),
            StructField("AQI_Category", StringType(), True),
            StructField("CO_AQI", IntegerType(), True),
            StructField("Ozone_AQI", IntegerType(), True),
            StructField("NO2_AQI", IntegerType(), True),
            StructField("PM2_5_AQI", IntegerType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        return self.spark \
            .readStream \
            .format("json") \
            .schema(schema) \
            .option("path", hdfs_path) \
            .option("maxFilesPerTrigger", "1") \
            .load()
    
    def streaming_operation_1_data_streaming(self, df):
        """Streaming Operation 1: Data Streaming - Real-time data ingestion and processing"""
        return df \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("data_source", lit("HDFS_Stream")) \
            .filter(col("AQI_Value").isNotNull()) \
            .select(
                col("Country").alias("COUNTRY"),
                col("City").alias("CITY"),
                col("AQI_Value").alias("AQI"),
                col("AQI_Category").alias("CATEGORY"),
                date_format(col("timestamp"), "HH:mm:ss").alias("TIME"),
                col("data_source").alias("SOURCE")
            )
    
    def streaming_operation_2_aggregation(self, df):
        """Streaming Operation 2: Aggregation - Real-time statistical aggregations"""
        return df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "2 minutes"),
                col("Country")
            ) \
            .agg(
                avg("AQI_Value").alias("avg_aqi"),
                max("AQI_Value").alias("max_aqi"),
                min("AQI_Value").alias("min_aqi"),
                count("*").alias("record_count")
            ) \
            .select(
                col("Country"),
                date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
                date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
                round(col("avg_aqi"), 2).alias("avg_aqi"),
                col("max_aqi"),
                col("min_aqi"),
                col("record_count")
            )
    
    def streaming_operation_3_transformation(self, df):
        """Streaming Operation 3: Transformation - Data structure and format transformations"""
        return df \
            .withColumn("aqi_normalized", round(col("AQI_Value") / 500.0, 3)) \
            .withColumn("pollution_level",
                when(col("AQI_Value") <= 50, 1)
                .when(col("AQI_Value") <= 100, 2)
                .when(col("AQI_Value") <= 150, 3)
                .when(col("AQI_Value") <= 200, 4)
                .when(col("AQI_Value") <= 300, 5)
                .otherwise(6)
            ) \
            .withColumn("air_status",
                when(col("AQI_Value") <= 50, "GOOD")
                .when(col("AQI_Value") <= 100, "MODERATE")
                .when(col("AQI_Value") <= 200, "UNHEALTHY")
                .otherwise("HAZARDOUS")
            ) \
            .select(
                col("Country"),
                col("City"),
                col("AQI_Value").alias("original_aqi"),
                col("aqi_normalized"),
                col("pollution_level"),
                col("air_status"),
                date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("measurement_time")
            )
    
    def write_to_console_table(self, df, query_name, operation_name):
        """Write streaming results to console in nice table format"""
        print(f"\n{'='*60}")
        print(f" STREAMING OPERATION: {operation_name.upper()}")
        print(f"{'='*60}")
        
        return df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 15) \
            .queryName(query_name) \
            .trigger(processingTime='15 seconds') \
            .start()
    
    def write_to_hdfs_json(self, df, output_path, checkpoint_path, query_name):
        """Write streaming results to HDFS in JSON format"""
        return df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .queryName(query_name) \
            .trigger(processingTime='15 seconds') \
            .start()
    
    def run_all_operations(self, input_path, output_base_path):
        """Run all three streaming operations"""
        try:
            print(" Starting Air Quality Streaming Pipeline...")
            print(f" Input Path: {input_path}")
            print(f" Output Path: {output_base_path}")
            
            
            raw_stream = self.read_from_hdfs_stream(input_path)
            
            # Operation 1: Data Streaming (Console Output)
            streamed_data = self.streaming_operation_1_data_streaming(raw_stream)
            query1 = self.write_to_console_table(streamed_data, "data_streaming", "Data Streaming")
            
            # Operation 2: Aggregation (HDFS Output)
            aggregated_data = self.streaming_operation_2_aggregation(raw_stream)
            query2 = self.write_to_hdfs_json(
                aggregated_data,
                f"{output_base_path}/aggregated_data",
                f"{output_base_path}/checkpoints/aggregated",
                "aggregation"
            )
            
            # Operation 3: Transformation (HDFS Output)
            transformed_data = self.streaming_operation_3_transformation(raw_stream)
            query3 = self.write_to_hdfs_json(
                transformed_data,
                f"{output_base_path}/transformed_data",
                f"{output_base_path}/checkpoints/transformed",
                "transformation"
            )
            
            print("\n All streaming operations started successfully!")
            print(" Operation 1: Data Streaming → Console (Table View)")
            print(" Operation 2: Aggregation → HDFS JSON")
            print(" Operation 3: Transformation → HDFS JSON")
            print(f" Monitor at: http://localhost:4041")
            print("\n Streaming in progress... Press Ctrl+C to stop")
            
            
            query1.awaitTermination()
            
        except KeyboardInterrupt:
            print("\n Stopping streaming pipeline...")
            self.stop_all_streams()
        except Exception as e:
            print(f" Error in streaming pipeline: {str(e)}")
            self.stop_all_streams()
        finally:
            print("🔚 Streaming pipeline stopped.")
            self.spark.stop()
    
    def stop_all_streams(self):
        """Stop all active streams"""
        for stream in self.spark.streams.active:
            print(f" Stopping stream: {stream.name}")
            stream.stop()