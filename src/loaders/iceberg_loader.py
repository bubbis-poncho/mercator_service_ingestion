
import logging
import os
from typing import Dict, Any, Optional, List, Union
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class IcebergLoader(BaseLoader):
    """
    Loader for writing data to Iceberg tables using Apache Spark.
    """
    
    def __init__(self, 
                 config: Dict[str, Any]):
        """
        Initialize Iceberg loader.
        
        Args:
            config: Loader configuration containing:
                - spark_config: Spark configuration options
                - catalog_name: Catalog name for Iceberg tables (default: "hive_prod")
                - warehouse_location: Data warehouse location (optional)
        """
        super().__init__(config)
        self.spark_config = config.get("spark_config", {})
        self.catalog_name = config.get("catalog_name", "hive_prod")
        self.warehouse_location = config.get("warehouse_location") or os.environ.get("ICEBERG_WAREHOUSE_LOCATION")
        self.spark = None
    
    def connect(self) -> bool:
        """
        Initialize Spark session with Iceberg support.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.spark is not None:
                return True
                
            # Create builder with app name
            app_name = self.config.get("app_name", "Iceberg-Data-Loader")
            builder = SparkSession.builder.appName(app_name)
            
            # Add Spark configs
            for key, value in self.spark_config.items():
                builder = builder.config(key, value)
            
            # Add Iceberg-specific configurations
            builder = builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            builder = builder.config("spark.sql.catalog." + self.catalog_name, "org.apache.iceberg.spark.SparkCatalog")
            builder = builder.config("spark.sql.catalog." + self.catalog_name + ".type", "hive")
            
            if self.warehouse_location:
                builder = builder.config(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.warehouse_location)
            
            # Create session
            self.spark = builder.getOrCreate()
            logger.info("Spark session initialized with Iceberg support")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close Spark session."""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
            logger.info("Spark session closed")
    
    def create_table_if_not_exists(self, 
                                   database: str, 
                                   table_name: str, 
                                   schema: Optional[Union[StructType, str]] = None) -> bool:
        """
        Create Iceberg table if it doesn't exist.
        
        Args:
            database: Database name
            table_name: Table name
            schema: Optional schema as PySpark StructType or SQL schema string
            
        Returns:
            True if table exists or was created, False otherwise
        """
        try:
            if not self.connect():
                return False
            
            # Check if database exists, create if not
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
            
            # Full table identifier
            full_table_name = f"{database}.{table_name}"
            catalog_table = f"{self.catalog_name}.{full_table_name}"
            
            # Check if table exists
            tables = self.spark.sql(f"SHOW TABLES IN {database}").filter(f"tableName = '{table_name}'")
            
            if tables.count() == 0:
                logger.info(f"Creating Iceberg table: {catalog_table}")
                
                # Create table based on provided schema or default schema
                if isinstance(schema, StructType):
                    # Use PySpark DataFrame API to create table with StructType
                    empty_df = self.spark.createDataFrame([], schema)
                    empty_df.write.format("iceberg").saveAsTable(catalog_table)
                    
                elif isinstance(schema, str):
                    # Use SQL with provided schema string
                    self.spark.sql(f"""
                    CREATE TABLE {catalog_table} (
                        {schema}
                    )
                    USING iceberg
                    """)
                    
                else:
                    # Use default schema if none provided
                    self.spark.sql(f"""
                    CREATE TABLE {catalog_table} (
                        source STRING,
                        data_timestamp TIMESTAMP,
                        ingestion_timestamp TIMESTAMP
                    )
                    USING iceberg
                    """)
                
                logger.info(f"Successfully created Iceberg table: {catalog_table}")
            else:
                logger.info(f"Table already exists: {catalog_table}")
                
            return True
            
        except Exception as e:
            logger.error(f"Error creating table {database}.{table_name}: {str(e)}")
            return False
    
    def load_data(self, 
                 data: pd.DataFrame, 
                 database: str, 
                 table_name: str,
                 mode: str = "append") -> int:
        """
        Load data into Iceberg table.
        
        Args:
            data: DataFrame to load
            database: Database name
            table_name: Table name
            mode: Write mode ('append', 'overwrite', 'ignore', 'error')
            
        Returns:
            Number of rows written
        """
        try:
            if data.empty:
                logger.warning("No data to load")
                return 0
                
            if not self.connect():
                logger.error("Failed to connect to Spark/Iceberg")
                return 0
            
            # Full table identifier
            full_table_name = f"{self.catalog_name}.{database}.{table_name}"
            
            # Check if table exists, create if not
            if not self.create_table_if_not_exists(database, table_name):
                logger.error(f"Failed to ensure table {database}.{table_name} exists")
                return 0
            
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.spark.createDataFrame(data)
            
            # Write to Iceberg table
            logger.info(f"Writing {len(data)} rows to {full_table_name} using mode: {mode}")
            spark_df.write.format("iceberg").mode(mode).save(full_table_name)
            
            # Verify write
            count = self.spark.read.format("iceberg").load(full_table_name).count()
            logger.info(f"After load, table {full_table_name} has {count} rows")
            
            return len(data)
            
        except Exception as e:
            logger.error(f"Error loading data to {database}.{table_name}: {str(e)}")
            return 0
    
    def create_fsa_insider_trading_table(self, database: str, table_name: str) -> bool:
        """
        Create Iceberg table specifically for FSA insider trading data.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            True if table was created or exists, False otherwise
        """
        schema = """
            publication_date TIMESTAMP,
            issuer STRING,
            person_in_leading_position STRING,
            position STRING,
            related_party STRING,
            transaction_type STRING,
            instrument_name STRING,
            instrument_type STRING,
            isin STRING,
            transaction_date TIMESTAMP,
            volume DOUBLE,
            volume_unit STRING,
            price DOUBLE,
            currency STRING,
            status STRING,
            details_link STRING,
            source STRING,
            ingestion_timestamp TIMESTAMP
        """
        
        # Create table with specific partitioning for FSA data
        try:
            if not self.connect():
                return False
                
            # Check if database exists, create if not
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
            
            # Full table identifier
            full_table_name = f"{database}.{table_name}"
            catalog_table = f"{self.catalog_name}.{full_table_name}"
            
            # Check if table exists
            tables = self.spark.sql(f"SHOW TABLES IN {database}").filter(f"tableName = '{table_name}'")
            
            if tables.count() == 0:
                logger.info(f"Creating FSA insider trading Iceberg table: {catalog_table}")
                
                # Create table with FSA-specific schema and partitioning
                self.spark.sql(f"""
                CREATE TABLE {catalog_table} (
                    {schema}
                )
                USING iceberg
                PARTITIONED BY (days(transaction_date))
                """)
                
                logger.info(f"Successfully created FSA insider trading table: {catalog_table}")
            else:
                logger.info(f"FSA insider trading table already exists: {catalog_table}")
                
            return True
            
        except Exception as e:
            logger.error(f"Error creating FSA table {database}.{table_name}: {str(e)}")
            return False