# Data Pipeline Project

This project implements a data pipeline that stores data in S3 and uses Apache Iceberg for querying.

## Project Structure
```
.
├── README.md
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── s3_utils.py
│   └── iceberg_utils.py
└── config/
    └── .env.example
```

## Setup Instructions

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
   - Copy `config/.env.example` to `config/.env`
   - Fill in your AWS credentials and S3 bucket information

## Components

### S3 Storage
- Utilities for reading and writing data to S3
- Data format conversion and storage

### Apache Iceberg
- Table creation and management
- Query interface for the stored data

## Usage

[To be added as we implement the components] 