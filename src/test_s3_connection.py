import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from s3_utils import S3Handler

def generate_sample_data(num_records: int = 100) -> pd.DataFrame:
    """
    Generate sample transaction data
    """
    # Generate random dates for the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]
    
    data = {
        'transaction_id': [f'TXN{i:06d}' for i in range(num_records)],
        'amount': np.random.uniform(10, 1000, num_records).round(2),
        'transaction_date': np.random.choice(dates, num_records),
        'merchant_id': [f'MERCH{np.random.randint(1000, 9999)}' for _ in range(num_records)],
        'status': np.random.choice(['completed', 'pending', 'failed'], num_records)
    }
    
    return pd.DataFrame(data)

def main():
    # Initialize S3 handler
    s3_handler = S3Handler()
    
    # Generate sample data
    print("Generating sample data...")
    df = generate_sample_data(50)
    print(f"Generated {len(df)} records")
    
    # Test upload
    test_key = f"test_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\nUploading data to S3 with key: {test_key}")
    upload_success = s3_handler.upload_dataframe(df, test_key)
    
    if upload_success:
        print("Upload successful!")
        
        # Test reading back
        print("\nReading data back from S3...")
        read_df = s3_handler.read_parquet(test_key)
        
        if not read_df.empty:
            print("Successfully read data back from S3!")
            print("\nFirst few records:")
            print(read_df.head())
            
            # Verify data integrity
            if len(read_df) == len(df):
                print("\nData integrity check passed: Record count matches")
            else:
                print("\nData integrity check failed: Record count mismatch")
        else:
            print("Failed to read data from S3")
    else:
        print("Failed to upload data to S3")
    
    # List files in the bucket
    print("\nListing files in the bucket:")
    files = s3_handler.list_files()
    for file in files:
        print(f"- {file}")

if __name__ == "__main__":
    main() 