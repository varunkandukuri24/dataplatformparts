import os
from typing import List, Dict, Any
import pyiceberg
from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/.env')

class IcebergHandler:
    def __init__(self):
        self.catalog_name = os.getenv('ICEBERG_CATALOG_NAME')
        self.warehouse_path = os.getenv('ICEBERG_WAREHOUSE_PATH')
        
        # Initialize catalog
        self.catalog = load_catalog(
            self.catalog_name,
            **{
                'uri': self.warehouse_path,
                's3.endpoint': f"https://s3.{os.getenv('AWS_REGION')}.amazonaws.com",
                's3.access-key-id': os.getenv('AWS_ACCESS_KEY_ID'),
                's3.secret-access-key': os.getenv('AWS_SECRET_ACCESS_KEY')
            }
        )

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """
        Create a new Iceberg table
        """
        try:
            # Convert schema dict to Iceberg schema
            iceberg_schema = pyiceberg.schema.Schema(
                *[pyiceberg.types.NestedField.required(i, name, self._get_iceberg_type(type_str))
                  for i, (name, type_str) in enumerate(schema.items())]
            )
            
            # Create table
            self.catalog.create_table(
                identifier=table_name,
                schema=iceberg_schema,
                properties={'format-version': '2'}
            )
            return True
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            return False

    def append_to_table(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """
        Append data to an existing Iceberg table
        """
        try:
            table = self.catalog.load_table(table_name)
            table.append(data)
            return True
        except Exception as e:
            print(f"Error appending to table: {str(e)}")
            return False

    def query_table(self, table_name: str, query: str) -> List[Dict[str, Any]]:
        """
        Query data from an Iceberg table
        """
        try:
            table = self.catalog.load_table(table_name)
            return table.scan().filter(query).to_pandas().to_dict('records')
        except Exception as e:
            print(f"Error querying table: {str(e)}")
            return []

    def _get_iceberg_type(self, type_str: str) -> pyiceberg.types.PrimitiveType:
        """
        Convert string type to Iceberg type
        """
        type_map = {
            'string': pyiceberg.types.StringType(),
            'integer': pyiceberg.types.IntegerType(),
            'long': pyiceberg.types.LongType(),
            'float': pyiceberg.types.FloatType(),
            'double': pyiceberg.types.DoubleType(),
            'boolean': pyiceberg.types.BooleanType(),
            'date': pyiceberg.types.DateType(),
            'timestamp': pyiceberg.types.TimestampType()
        }
        return type_map.get(type_str.lower(), pyiceberg.types.StringType()) 