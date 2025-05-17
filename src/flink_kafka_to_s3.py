"""
Simple Flink job to read from Kafka and write to S3.
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
import pandas as pd
from s3_utils import S3Handler

# Load environment variables
load_dotenv('config/.env')

def create_kafka_source():
    """Create and configure Kafka source"""
    return KafkaSource.builder() \
        .set_bootstrap_servers(os.getenv('KAFKA_BOOTSTRAP_SERVERS')) \
        .set_topics(os.getenv('KAFKA_TOPIC')) \
        .set_group_id(os.getenv('KAFKA_GROUP_ID')) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

def process_record(record):
    """Process each record from Kafka"""
    try:
        # Parse JSON record
        data = json.loads(record)
        
        # Add processing timestamp
        data['processing_timestamp'] = datetime.utcnow().isoformat()
        
        return data
    except Exception as e:
        print(f"Error processing record: {str(e)}")
        return None

def write_to_s3(records, s3_handler):
    """Write records to S3"""
    if not records:
        return
    
    try:
        # Convert records to DataFrame
        df = pd.DataFrame(records)
        
        # Generate S3 key with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        s3_key = f"kafka_data/{timestamp}"
        
        # Upload to S3
        s3_handler.upload_dataframe(df, s3_key)
        print(f"Successfully wrote {len(records)} records to S3: {s3_key}")
    except Exception as e:
        print(f"Error writing to S3: {str(e)}")

def main():
    # Initialize Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Initialize S3 handler
    s3_handler = S3Handler()
    
    # Create Kafka source
    kafka_source = create_kafka_source()
    
    # Create data stream
    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )
    
    # Process records
    processed_stream = stream \
        .map(process_record, output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .filter(lambda x: x is not None)
    
    # Add sink to write to S3
    processed_stream.add_sink(
        lambda records: write_to_s3(records, s3_handler)
    )
    
    # Execute the job
    env.execute("Kafka to S3 Pipeline")

if __name__ == '__main__':
    main() 