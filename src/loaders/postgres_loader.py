import logging
import os
from typing import Dict, List, Optional, Any, Union, Tuple
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, inspect, text
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
import hashlib
import json
from datetime import datetime
from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class PostgresLoader(BaseLoader):
    """
    Enhanced loader for writing data to PostgreSQL databases with advanced features like:
    - Schema creation
    - Table creation with auto-detected or custom schemas
    - Flexible write modes: append, overwrite, merge (upsert)
    - Partitioning support
    - Transaction handling
    - Batch processing for large datasets
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL loader.
        
        Args:
            config: Loader configuration containing:
                - host: Database host (required)
                - port: Database port (default: 5432)
                - username: Database username (required)
                - password: Database password (required)
                - database: Default database name (required)
                - schema: Default schema name (default: "public")
                - connection_options: Additional connection options
                - batch_size: Number of rows per batch (default: 10000)
                - create_schema_if_not_exists: Create schema if it doesn't exist (default: True)
                - use_batch_mode: Use batch mode for data loading (default: True)
                - on_conflict: Action on conflict for upserts (default: "update")
                - use_transactions: Use transactions for data loading (default: True)
        """
        super().__init__(config)
        
        # Required parameters
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 5432)
        self.username = config.get("username")
        self.password = config.get("password")
        self.database = config.get("database")
        
        # Optional parameters
        self.schema = config.get("schema", "public")
        self.connection_options = config.get("connection_options", {})
        self.batch_size = config.get("batch_size", 10000)
        self.create_schema_if_not_exists = config.get("create_schema_if_not_exists", True)
        self.use_batch_mode = config.get("use_batch_mode", True)
        self.on_conflict = config.get("on_conflict", "update")  # or "ignore", "nothing"
        self.use_transactions = config.get("use_transactions", True)
        
        # SQLAlchemy objects
        self.engine = None
        self.metadata = None
        
        # Validate required parameters
        if not all([self.username, self.password, self.database]):
            raise ValueError("Missing required PostgreSQL connection parameters (username, password, database)")
    
    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.engine is not None:
                return True
            
            # Build connection string
            connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
            
            # Add connection options
            if self.connection_options:
                options_str = "&".join([f"{k}={v}" for k, v in self.connection_options.items()])
                connection_string = f"{connection_string}?{options_str}"
            
            # Create engine
            self.engine = create_engine(connection_string)
            self.metadata = MetaData(schema=self.schema)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")
            
            # Create schema if it doesn't exist and configured to do so
            if self.create_schema_if_not_exists and self.schema != "public":
                self._create_schema_if_not_exists(self.schema)
            
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
    
    def _create_schema_if_not_exists(self, schema: str) -> None:
        """
        Create schema if it doesn't exist.
        
        Args:
            schema: Schema name
        """
        try:
            with self.engine.connect() as conn:
                # Check if schema exists
                result = conn.execute(text(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name"
                ), {"schema_name": schema})
                
                if not result.fetchone():
                    # Create schema
                    conn.execute(CreateSchema(schema))
                    conn.commit()
                    logger.info(f"Created schema: {schema}")
        except Exception as e:
            logger.error(f"Error creating schema {schema}: {str(e)}")
            raise
    
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
            
            # Create schema if it doesn't exist and configured to do so
            if self.create_schema_if_not_exists and schema_name != "public":
                self._create_schema_if_not_exists(schema_name)
            
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
                mode: str = "append",
                merge_keys: Optional[List[str]] = None) -> int:
        """
        Load data into PostgreSQL table.
        
        Args:
            data: DataFrame to load
            database: Schema name
            table_name: Table name
            mode: Write mode ('append', 'replace', 'merge')
            merge_keys: List of columns to use as merge keys for 'merge' mode
            
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
            
            # Ensure schema exists
            if self.create_schema_if_not_exists and database != "public":
                self._create_schema_if_not_exists(database)
            
            # Ensure table exists - infer schema from DataFrame
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
            
            # Handle different write modes
            if mode == "append":
                return self._load_append(data, database, table_name)
            elif mode == "replace":
                return self._load_replace(data, database, table_name)
            elif mode == "merge":
                if not merge_keys:
                    logger.error("Merge keys must be specified for 'merge' mode")
                    return 0
                return self._load_merge(data, database, table_name, merge_keys)
            else:
                logger.error(f"Unsupported write mode: {mode}")
                return 0
        except Exception as e:
            logger.error(f"Error loading data to {database}.{table_name}: {str(e)}")
            return 0
    
    def _load_append(self, data: pd.DataFrame, database: str, table_name: str) -> int:
        """
        Append data to table.
        
        Args:
            data: DataFrame to load
            database: Schema name
            table_name: Table name
            
        Returns:
            Number of rows written
        """
        logger.info(f"Appending {len(data)} rows to {database}.{table_name}")
        
        if self.use_batch_mode and len(data) > self.batch_size:
            # Load in batches
            total_rows = 0
            for i in range(0, len(data), self.batch_size):
                batch = data.iloc[i:i+self.batch_size]
                rows_written = self._write_batch(batch, database, table_name, "append")
                total_rows += rows_written
                logger.info(f"Loaded batch {i//self.batch_size + 1}: {rows_written} rows")
            return total_rows
        else:
            # Load all at once
            return self._write_batch(data, database, table_name, "append")
    
    def _load_replace(self, data: pd.DataFrame, database: str, table_name: str) -> int:
        """
        Replace table data.
        
        Args:
            data: DataFrame to load
            database: Schema name
            table_name: Table name
            
        Returns:
            Number of rows written
        """
        logger.info(f"Replacing data in {database}.{table_name} with {len(data)} rows")
        
        # Use a transaction for replace operation
        with self.engine.begin() as conn:
            # Delete existing data
            conn.execute(text(f'TRUNCATE TABLE {database}.{table_name}'))
            
            # Insert new data
            if self.use_batch_mode and len(data) > self.batch_size:
                # Load in batches within the transaction
                total_rows = 0
                for i in range(0, len(data), self.batch_size):
                    batch = data.iloc[i:i+self.batch_size]
                    batch.to_sql(
                        table_name, 
                        conn, 
                        schema=database, 
                        if_exists="append", 
                        index=False
                    )
                    total_rows += len(batch)
                    logger.info(f"Loaded batch {i//self.batch_size + 1}: {len(batch)} rows")
                return total_rows
            else:
                # Load all at once within the transaction
                data.to_sql(
                    table_name, 
                    conn, 
                    schema=database, 
                    if_exists="append", 
                    index=False
                )
                return len(data)
    
    def _load_merge(self, 
                   data: pd.DataFrame, 
                   database: str, 
                   table_name: str, 
                   merge_keys: List[str]) -> int:
        """
        Merge (upsert) data into table.
        
        Args:
            data: DataFrame to load
            database: Schema name
            table_name: Table name
            merge_keys: List of columns to use as merge keys
            
        Returns:
            Number of rows written
        """
        logger.info(f"Merging {len(data)} rows into {database}.{table_name} using keys: {merge_keys}")
        
        # Validate merge keys
        for key in merge_keys:
            if key not in data.columns:
                logger.error(f"Merge key {key} not found in data")
                return 0
        
        # Generate SQL for upsert
        # PostgreSQL's ON CONFLICT syntax for upsert
        
        # Prepare column names for SQL
        columns = list(data.columns)
        column_names = ', '.join(f'"{col}"' for col in columns)
        
        # Prepare values placeholder
        placeholders = ', '.join([f':{col}' for col in columns])
        
        # Prepare set clause for updates
        update_columns = [col for col in columns if col not in merge_keys]
        if not update_columns:
            logger.error("No columns to update (all columns are merge keys)")
            return 0
        
        set_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
        
        # Prepare conflict target
        conflict_target = ', '.join([f'"{key}"' for key in merge_keys])
        
        # Determine conflict action
        if self.on_conflict == "update":
            conflict_action = f"DO UPDATE SET {set_clause}"
        elif self.on_conflict == "nothing":
            conflict_action = "DO NOTHING"
        else:
            logger.error(f"Unsupported on_conflict action: {self.on_conflict}")
            return 0
        
        # Build the upsert SQL
        upsert_sql = f"""
        INSERT INTO {database}.{table_name} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_target})
        {conflict_action}
        """
        
        total_rows = 0
        
        # Use transaction if configured
        conn = self.engine.begin() if self.use_transactions else self.engine.connect()
        
        try:
            with conn as connection:
                if self.use_batch_mode and len(data) > self.batch_size:
                    # Process in batches
                    for i in range(0, len(data), self.batch_size):
                        batch = data.iloc[i:i+self.batch_size]
                        
                        # Convert batch to list of dictionaries
                        records = batch.to_dict(orient='records')
                        
                        # Execute batch upsert
                        result = connection.execute(text(upsert_sql), records)
                        rows_affected = result.rowcount
                        
                        total_rows += rows_affected
                        logger.info(f"Merged batch {i//self.batch_size + 1}: {rows_affected} rows affected")
                else:
                    # Process all at once
                    records = data.to_dict(orient='records')
                    result = connection.execute(text(upsert_sql), records)
                    total_rows = result.rowcount
        except Exception as e:
            logger.error(f"Error during merge operation: {str(e)}")
            raise
        finally:
            if not self.use_transactions and conn:
                conn.close()
        
        return total_rows
    
    def _write_batch(self, 
                    data: pd.DataFrame, 
                    database: str, 
                    table_name: str, 
                    if_exists: str) -> int:
        """
        Write a batch of data to PostgreSQL.
        
        Args:
            data: DataFrame to write
            database: Schema name
            table_name: Table name
            if_exists: SQLAlchemy if_exists option
            
        Returns:
            Number of rows written
        """
        if self.use_transactions:
            with self.engine.begin() as conn:
                data.to_sql(
                    table_name, 
                    conn, 
                    schema=database, 
                    if_exists=if_exists, 
                    index=False,
                    chunksize=self.batch_size
                )
        else:
            data.to_sql(
                table_name, 
                self.engine, 
                schema=database, 
                if_exists=if_exists, 
                index=False,
                chunksize=self.batch_size
            )
        
        return len(data)
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a DataFrame.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            Query results as a pandas DataFrame
        """
        try:
            if not self.connect():
                raise Exception("Failed to connect to PostgreSQL")
            
            return pd.read_sql(text(query), self.engine, params=params)
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def table_exists(self, database: str, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            database: Schema name
            table_name: Table name
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            if not self.connect():
                return False
            
            inspector = inspect(self.engine)
            return inspector.has_table(table_name, schema=database)
        except Exception as e:
            logger.error(f"Error checking if table exists: {str(e)}")
            return False
    
    def delete_data(self, 
                   database: str, 
                   table_name: str, 
                   condition: Optional[str] = None, 
                   params: Optional[Dict] = None) -> int:
        """
        Delete data from a table.
        
        Args:
            database: Schema name
            table_name: Table name
            condition: Optional WHERE condition
            params: Parameters for the WHERE condition
            
        Returns:
            Number of rows deleted
        """
        try:
            if not self.connect():
                return 0
            
            delete_sql = f"DELETE FROM {database}.{table_name}"
            
            if condition:
                delete_sql += f" WHERE {condition}"
            
            with self.engine.begin() as conn:
                result = conn.execute(text(delete_sql), params or {})
                rows_deleted = result.rowcount
                
                logger.info(f"Deleted {rows_deleted} rows from {database}.{table_name}")
                return rows_deleted
        except Exception as e:
            logger.error(f"Error deleting data: {str(e)}")
            return 0
    
    def calculate_table_hash(self, database: str, table_name: str) -> str:
        """
        Calculate a hash of table structure for detecting schema changes.
        
        Args:
            database: Schema name
            table_name: Table name
            
        Returns:
            Hash string representing the table structure
        """
        try:
            if not self.connect():
                return ""
            
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=database)
            
            # Create a deterministic representation of the schema
            schema_repr = []
            for column in sorted(columns, key=lambda x: x['name']):
                col_info = {
                    'name': column['name'],
                    'type': str(column['type']),
                    'nullable': column.get('nullable', True)
                }
                schema_repr.append(col_info)
            
            # Create a hash of the schema representation
            schema_str = json.dumps(schema_repr, sort_keys=True)
            return hashlib.md5(schema_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error calculating table hash: {str(e)}")
            return ""