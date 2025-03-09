
import logging
import os
from typing import Dict, Any, Optional, List, Union
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, inspect
from sqlalchemy.ext.declarative import declarative_base

from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class PostgresLoader(BaseLoader):
    """
    Loader for writing data to PostgreSQL databases.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL loader.
        
        Args:
            config: Loader configuration containing:
                - host: Database host
                - port: Database port
                - user: Database username
                - password: Database password
                - database: Default database name
                - schema: Default schema name (optional)
        """
        super().__init__(config)
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 5432)
        self.user = config.get("user", "postgres")
        self.password = config.get("password", "")
        self.database = config.get("database", "postgres")
        self.schema = config.get("schema", "public")
        self.engine = None
        self.metadata = None
        self.Base = declarative_base()
    
    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.engine is not None:
                return True
                
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(connection_string)
            self.metadata = MetaData(schema=self.schema)
            
            # Test connection
            conn = self.engine.connect()
            conn.close()
            
            logger.info(f"Connected to PostgreSQL database at {self.host}:{self.port}/{self.database}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            logger.info("PostgreSQL connection closed")
    
    def create_table_if_not_exists(self, 
                                  database: str, 
                                  table_name: str,
                                  schema: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create PostgreSQL table if it doesn't exist.
        
        Args:
            database: Schema name (PostgreSQL schema, not database)
            table_name: Table name
            schema: Dictionary mapping column names to SQLAlchemy types
            
        Returns:
            True if table exists or was created, False otherwise
        """
        try:
            if not self.connect():
                return False
                
            # In PostgreSQL, 'database' parameter is used as schema name
            schema_name = database
            
            # Check if table exists
            inspector = inspect(self.engine)
            if inspector.has_table(table_name, schema=schema_name):
                logger.info(f"Table {schema_name}.{table_name} already exists")
                return True
                
            # Create table
            logger.info(f"Creating table {schema_name}.{table_name}")
            
            metadata = MetaData(schema=schema_name)
            
            # Generate columns from schema dict
            columns = []
            if schema:
                for column_name, column_type in schema.items():
                    columns.append(Column(column_name, column_type))
            else:
                # Default minimal schema
                columns = [
                    Column('id', sqlalchemy.Integer, primary_key=True),
                    Column('source', sqlalchemy.String),
                    Column('data_timestamp', sqlalchemy.DateTime),
                    Column('ingestion_timestamp', sqlalchemy.DateTime)
                ]
                
            # Create table definition
            table = Table(table_name, metadata, *columns)
            
            # Create table in database
            metadata.create_all(self.engine)
            logger.info(f"Successfully created table {schema_name}.{table_name}")
            
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
        Load data into PostgreSQL table.
        
        Args:
            data: DataFrame to load
            database: Schema name
            table_name: Table name
            mode: Write mode ('append', 'replace')
            
        Returns:
            Number of rows written
        """
        try:
            if data.empty:
                logger.warning("No data to load")
                return 0
                
            if not self.connect():
                logger.error("Failed to connect to PostgreSQL")
                return 0
                
            # Ensure table exists
            # Infer schema from DataFrame
            schema = {}
            for column, dtype in data.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    schema[column] = sqlalchemy.Integer
                elif pd.api.types.is_float_dtype(dtype):
                    schema[column] = sqlalchemy.Float
                elif pd.api.types.is_datetime64_dtype(dtype):
                    schema[column] = sqlalchemy.DateTime
                elif pd.api.types.is_bool_dtype(dtype):
                    schema[column] = sqlalchemy.Boolean
                else:
                    schema[column] = sqlalchemy.String
            
            if not self.create_table_if_not_exists(database, table_name, schema):
                logger.error(f"Failed to ensure table {database}.{table_name} exists")
                return 0
                
            # Write DataFrame to PostgreSQL
            logger.info(f"Writing {len(data)} rows to {database}.{table_name} using mode: {mode}")
            data.to_sql(
                table_name, 
                self.engine, 
                schema=database, 
                if_exists=mode, 
                index=False, 
                chunksize=1000
            )
            
            # Verify row count
            with self.engine.connect() as conn:
                result = conn.execute(f'SELECT COUNT(*) FROM {database}.{table_name}')
                count = result.scalar()
                logger.info(f"After load, table {database}.{table_name} has {count} rows")
            
            return len(data)
            
        except Exception as e:
            logger.error(f"Error loading data to {database}.{table_name}: {str(e)}")
            return 0


# backend/ingestion-service/src/loaders/delta_lake_loader.py

import logging
import os
from typing import Dict, Any, Optional, List, Union
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class DeltaLakeLoader(BaseLoader):
    """
    Loader for writing data to Delta Lake tables using Apache Spark.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Delta Lake loader.
        
        Args:
            config: Loader configuration containing:
                - spark_config: Spark configuration options
                - warehouse_location: Data warehouse location (optional)
        """
        super().__init__(config)
        self.spark_config = config.get("spark_config", {})
        self.warehouse_location = config.get("warehouse_location") or os.environ.get("DELTA_WAREHOUSE_LOCATION")
        self.spark = None
    
    def connect(self) -> bool:
        """
        Initialize Spark session with Delta Lake support.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.spark is not None:
                return True
                
            # Create builder with app name
            app_name = self.config.get("app_name", "Delta-Lake-Data-Loader")
            builder = SparkSession.builder.appName(app_name)
            
            # Add Spark configs
            for key, value in self.spark_config.items():
                builder = builder.config(key, value)
            
            # Add Delta-specific configurations
            builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            if self.warehouse_location:
                builder = builder.config("spark.sql.warehouse.dir", self.warehouse_location)
            
            # Create session
            self.spark = builder.getOrCreate()
            logger.info("Spark session initialized with Delta Lake support")
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
        Create Delta Lake table if it doesn't exist.
        
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
            
            # Check if table exists
            tables = self.spark.sql(f"SHOW TABLES IN {database}").filter(f"tableName = '{table_name}'")
            
            if tables.count() == 0:
                logger.info(f"Creating Delta Lake table: {full_table_name}")
                
                # Create table based on provided schema or default schema
                if isinstance(schema, StructType):
                    # Use PySpark DataFrame API to create table with StructType
                    empty_df = self.spark.createDataFrame([], schema)
                    empty_df.write.format("delta").saveAsTable(full_table_name)
                    
                elif isinstance(schema, str):
                    # Use SQL with provided schema string
                    self.spark.sql(f"""
                    CREATE TABLE {full_table_name} (
                        {schema}
                    )
                    USING delta
                    """)
                    
                else:
                    # Use default schema if none provided
                    self.spark.sql(f"""
                    CREATE TABLE {full_table_name} (
                        source STRING,
                        data_timestamp TIMESTAMP,
                        ingestion_timestamp TIMESTAMP
                    )
                    USING delta
                    """)
                
                logger.info(f"Successfully created Delta Lake table: {full_table_name}")
            else:
                logger.info(f"Table already exists: {full_table_name}")
                
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
        Load data into Delta Lake table.
        
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
                logger.error("Failed to connect to Spark/Delta")
                return 0
            
            # Full table identifier
            full_table_name = f"{database}.{table_name}"
            
            # Check if table exists, create if not
            if not self.create_table_if_not_exists(database, table_name):
                logger.error(f"Failed to ensure table {database}.{table_name} exists")
                return 0
            
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.spark.createDataFrame(data)
            
            # Write to Delta Lake table
            logger.info(f"Writing {len(data)} rows to {full_table_name} using mode: {mode}")
            spark_df.write.format("delta").mode(mode).saveAsTable(full_table_name)
            
            # Verify write
            count = self.spark.read.format("delta").table(full_table_name).count()
            logger.info(f"After load, table {full_table_name} has {count} rows")
            
            return len(data)
            
        except Exception as e:
            logger.error(f"Error loading data to {database}.{table_name}: {str(e)}")
            return 0