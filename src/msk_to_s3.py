import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from s3_utils import S3Handler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('config/.env')

class MSKToS3Consumer:
    def __init__(self):
        """Initialize the MSK consumer and S3 handler."""
        # Initialize S3 handler
        self.s3_handler = S3Handler()
        
        # Configure Kafka consumer
        self.consumer_config = {
            'bootstrap.servers': os.getenv('MSK_BOOTSTRAP_SERVERS'),
            'group.id': os.getenv('MSK_GROUP_ID'),
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        }
        
        # Initialize consumer
        self.consumer = Consumer(self.consumer_config)
        self.topic = os.getenv('MSK_TOPIC')
        
        # Batch configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))
        self.batch_timeout = int(os.getenv('BATCH_TIMEOUT_SECONDS', '60'))
        
        # Validate configuration
        self._validate_config()

    def _validate_config(self):
        """Validate that all required configuration is present."""
        required_vars = [
            'MSK_BOOTSTRAP_SERVERS',
            'MSK_GROUP_ID',
            'MSK_TOPIC',
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'AWS_REGION',
            'S3_BUCKET_NAME'
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single message from MSK.
        
        Args:
            message: Raw message from Kafka
            
        Returns:
            Processed message as dictionary
        """
        try:
            # Parse JSON message
            data = json.loads(message.value().decode('utf-8'))
            
            # Add processing metadata
            data['processing_timestamp'] = datetime.utcnow().isoformat()
            data['kafka_topic'] = message.topic()
            data['kafka_partition'] = message.partition()
            data['kafka_offset'] = message.offset()
            
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON message: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return None

    def write_batch_to_s3(self, records: List[Dict[str, Any]]) -> None:
        """
        Write a batch of records to S3.
        
        Args:
            records: List of processed records to write
        """
        if not records:
            return
        
        try:
            # Convert to DataFrame
            import pandas as pd
            df = pd.DataFrame(records)
            
            # Generate S3 key with timestamp
            timestamp = datetime.utcnow()
            s3_key = f"msk_data/{timestamp.strftime('%Y/%m/%d/%H')}/data_{timestamp.strftime('%Y%m%d_%H%M%S')}"
            
            # Upload to S3
            success = self.s3_handler.upload_dataframe(df, s3_key)
            
            if success:
                logger.info(f"Successfully wrote {len(records)} records to S3: {s3_key}")
            else:
                logger.error(f"Failed to write batch to S3")
                
        except Exception as e:
            logger.error(f"Error writing batch to S3: {str(e)}")

    def run(self):
        """Run the consumer and process messages."""
        try:
            # Subscribe to topic
            self.consumer.subscribe([self.topic])
            logger.info(f"Subscribed to topic: {self.topic}")
            
            # Initialize batch
            current_batch = []
            last_batch_time = datetime.utcnow()
            
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Check if we should write the current batch due to timeout
                    if current_batch and (datetime.utcnow() - last_batch_time).seconds >= self.batch_timeout:
                        self.write_batch_to_s3(current_batch)
                        current_batch = []
                        last_batch_time = datetime.utcnow()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Error: {msg.error()}")
                    continue
                
                # Process message
                processed_msg = self.process_message(msg)
                if processed_msg:
                    current_batch.append(processed_msg)
                
                # Write batch if it reaches the size limit
                if len(current_batch) >= self.batch_size:
                    self.write_batch_to_s3(current_batch)
                    current_batch = []
                    last_batch_time = datetime.utcnow()
                
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        except Exception as e:
            logger.error(f"Error in consumer: {str(e)}")
        finally:
            # Write any remaining records
            if current_batch:
                self.write_batch_to_s3(current_batch)
            # Close consumer
            self.consumer.close()

def main():
    """Entry point for the MSK to S3 consumer."""
    try:
        consumer = MSKToS3Consumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Consumer failed: {str(e)}")
        raise

if __name__ == '__main__':
    main() 