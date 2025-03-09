# src/connectors/db_connector/base_connector.py
import logging
import time
from typing import Dict, List, Optional, Any, Union, Tuple
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import urllib.parse

logger = logging.getLogger(__name__)

class DBConnector:
    """Base database connector class with common functionality for all database connectors."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize base database connector with common configuration.
        
        Args:
            config: Database connector configuration containing:
                - db_type: Database type (postgres, mysql, sqlite, oracle, mssql, etc.)
                - host: Database server host
                - port: Database server port
                - username: Database username
                - password: Database password
                - database: Database name
                - schema: Database schema (optional)
                - connection_string: Full connection string (optional, overrides other params)
                - connection_params: Additional connection parameters
        """
        self.config = config
        self.db_type = config.get("db_type", "").lower()
        self.host = config.get("host", "localhost")
        self.port = config.get("port")
        self.username = config.get("username", "")
        self.password = config.get("password", "")
        self.database = config.get("database", "")
        self.schema = config.get("schema")
        self.connection_string = config.get("connection_string")
        self.connection_params = config.get("connection_params", {})
        
        self.engine = None
        self.connection = None
    
    def connect(self) -> bool:
        """
        Establish connection to the database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.engine is not None:
                return True
                
            if self.connection_string:
                # Use provided connection string
                conn_str = self.connection_string
            else:
                # Build connection string based on database type
                if self.db_type == "postgres":
                    port = self.port or 5432
                    password = urllib.parse.quote_plus(self.password) if self.password else ""
                    conn_str = f"postgresql://{self.username}:{password}@{self.host}:{port}/{self.database}"
                elif self.db_type == "mysql":
                    port = self.port or 3306
                    password = urllib.parse.quote_plus(self.password) if self.password else ""
                    conn_str = f"mysql+pymysql://{self.username}:{password}@{self.host}:{port}/{self.database}"
                elif self.db_type == "sqlite":
                    conn_str = f"sqlite:///{self.database}"
                elif self.db_type == "oracle":
                    port = self.port or 1521
                    password = urllib.parse.quote_plus(self.password) if self.password else ""
                    conn_str = f"oracle+cx_oracle://{self.username}:{password}@{self.host}:{port}/{self.database}"
                elif self.db_type == "mssql":
                    port = self.port or 1433
                    password = urllib.parse.quote_plus(self.password) if self.password else ""
                    conn_str = f"mssql+pyodbc://{self.username}:{password}@{self.host}:{port}/{self.database}"
                else:
                    raise ValueError(f"Unsupported database type: {self.db_type}")
                    
                # Add connection parameters if provided
                if self.connection_params:
                    params_str = "&".join([f"{k}={v}" for k, v in self.connection_params.items()])
                    conn_str = f"{conn_str}?{params_str}"
                    
            # Create engine with appropriate connection string
            self.engine = create_engine(conn_str)
            
            # Test connection
            self.connection = self.engine.connect()
            logger.info(f"Connected to {self.db_type} database at {self.host}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            
        logger.info("Database connection closed")
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query
            params: Optional query parameters
            
        Returns:
            Query results as a list of dictionaries
        """
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
                
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
                
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            raise
    
    def execute_query_df(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a DataFrame.
        
        Args:
            query: SQL query
            params: Optional query parameters
            
        Returns:
            Query results as a pandas DataFrame
        """
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
                
            return pd.read_sql(sql=text(query), con=self.engine, params=params or {})
                
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            raise
    
    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Table name
            schema: Optional schema name
            
        Returns:
            List of column definitions
        """
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
                
            # Use appropriate method based on database type
            if self.db_type == "postgres":
                query = """
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns
                WHERE table_name = :table_name
                """
                if schema:
                    query += " AND table_schema = :schema"
                    
                return self.execute_query(query, {"table_name": table_name, "schema": schema or self.schema})
                
            elif self.db_type == "mysql":
                query = """
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns
                WHERE table_name = :table_name
                """
                if schema:
                    query += " AND table_schema = :schema"
                    
                return self.execute_query(query, {"table_name": table_name, "schema": schema or self.schema or self.database})
                
            elif self.db_type == "sqlite":
                query = f"PRAGMA table_info({table_name})"
                result = self.execute_query(query)
                return [{"column_name": row["name"], "data_type": row["type"], "is_nullable": not row["notnull"]} for row in result]
                
            else:
                # Generic approach using SQLAlchemy reflection
                metadata = sqlalchemy.MetaData()
                metadata.reflect(bind=self.engine, only=[table_name], schema=schema or self.schema)
                table = metadata.tables.get(f"{schema or self.schema}.{table_name}" if schema or self.schema else table_name)
                
                if not table:
                    return []
                    
                return [
                    {
                        "column_name": col.name,
                        "data_type": str(col.type),
                        "is_nullable": col.nullable
                    }
                    for col in table.columns
                ]
                
        except Exception as e:
            logger.error(f"Error getting table schema: {str(e)}")
            return []
    
    def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        Get list of tables in the database or schema.
        
        Args:
            schema: Optional schema name
            
        Returns:
            List of table names
        """
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
                
            if self.db_type == "postgres" or self.db_type == "mysql":
                query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                """
                
                if schema:
                    query += " AND table_schema = :schema"
                elif self.schema:
                    query += " AND table_schema = :schema"
                    schema = self.schema
                else:
                    # Default schema for the database
                    if self.db_type == "postgres":
                        query += " AND table_schema = 'public'"
                    elif self.db_type == "mysql":
                        query += " AND table_schema = :schema"
                        schema = self.database
                
                result = self.execute_query(query, {"schema": schema} if schema else {})
                return [row["table_name"] for row in result]
                
            elif self.db_type == "sqlite":
                query = "SELECT name FROM sqlite_master WHERE type='table'"
                result = self.execute_query(query)
                return [row["name"] for row in result]
                
            else:
                # Generic approach using SQLAlchemy reflection
                metadata = sqlalchemy.MetaData()
                metadata.reflect(bind=self.engine, schema=schema or self.schema)
                return [table.name for table in metadata.tables.values() if table.schema == (schema or self.schema)]
                
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            return []
    
    def extract_data(self, 
                    table_name: str, 
                    schema: Optional[str] = None,
                    columns: Optional[List[str]] = None,
                    where_clause: Optional[str] = None,
                    params: Optional[Dict] = None,
                    batch_size: int = 10000) -> pd.DataFrame:
        """
        Extract data from a database table.
        
        Args:
            table_name: Source table name
            schema: Optional schema name
            columns: Optional list of columns to extract (default: all columns)
            where_clause: Optional WHERE clause for filtering
            params: Optional parameters for WHERE clause
            batch_size: Number of rows to fetch per batch
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
            
            # Prepare the query
            cols_str = ", ".join(columns) if columns else "*"
            schema_prefix = f"{schema}." if schema else f"{self.schema}." if self.schema else ""
            
            query = f"SELECT {cols_str} FROM {schema_prefix}{table_name}"
            
            if where_clause:
                query += f" WHERE {where_clause}"
            
            # For efficient processing of large tables, use batching with an orderBy clause
            if batch_size > 0 and self.db_type != "sqlite":
                # Get the primary key column(s) for the table
                pk_columns = self._get_primary_key_columns(table_name, schema)
                
                if pk_columns:
                    # Use the first primary key column for ordering
                    order_col = pk_columns[0]
                    batched_query = f"{query} ORDER BY {order_col} LIMIT {batch_size} OFFSET :offset"
                    
                    offset = 0
                    all_data = []
                    
                    while True:
                        batch_params = {"offset": offset}
                        if params:
                            batch_params.update(params)
                            
                        batch_df = self.execute_query_df(batched_query, batch_params)
                        
                        if batch_df.empty:
                            break
                            
                        all_data.append(batch_df)
                        offset += batch_size
                        
                        logger.info(f"Extracted batch of {len(batch_df)} rows from {schema_prefix}{table_name}, total rows: {offset}")
                        
                        if len(batch_df) < batch_size:
                            break
                    
                    if all_data:
                        return pd.concat(all_data, ignore_index=True)
                    else:
                        return pd.DataFrame()
                        
            # If batching not possible or for small tables, execute direct query
            return self.execute_query_df(query, params)
                
        except Exception as e:
            logger.error(f"Error extracting data from {table_name}: {str(e)}")
            raise
    
    def _get_primary_key_columns(self, table_name: str, schema: Optional[str] = None) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            table_name: Table name
            schema: Optional schema name
            
        Returns:
            List of primary key column names
        """
        try:
            if self.db_type == "postgres":
                query = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = :table_name::regclass AND i.indisprimary
                """
                
                if schema:
                    table_ref = f"{schema}.{table_name}"
                elif self.schema:
                    table_ref = f"{self.schema}.{table_name}"
                else:
                    table_ref = table_name
                    
                result = self.execute_query(query, {"table_name": table_ref})
                return [row["attname"] for row in result]
                
            elif self.db_type == "mysql":
                query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = :table_name AND CONSTRAINT_NAME = 'PRIMARY'
                """
                
                if schema:
                    query += " AND TABLE_SCHEMA = :schema"
                elif self.schema:
                    query += " AND TABLE_SCHEMA = :schema"
                    schema = self.schema
                else:
                    query += " AND TABLE_SCHEMA = :schema"
                    schema = self.database
                    
                result = self.execute_query(query, {"table_name": table_name, "schema": schema})
                return [row["COLUMN_NAME"] for row in result]
                
            elif self.db_type == "sqlite":
                query = f"PRAGMA table_info({table_name})"
                result = self.execute_query(query)
                return [row["name"] for row in result if row["pk"] == 1]
                
            else:
                # Generic approach using SQLAlchemy reflection
                metadata = sqlalchemy.MetaData()
                metadata.reflect(bind=self.engine, only=[table_name], schema=schema or self.schema)
                table = metadata.tables.get(f"{schema or self.schema}.{table_name}" if schema or self.schema else table_name)
                
                if not table or not table.primary_key:
                    return []
                    
                return [col.name for col in table.primary_key.columns]
                
        except Exception as e:
            logger.error(f"Error getting primary key columns: {str(e)}")
            return []
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

