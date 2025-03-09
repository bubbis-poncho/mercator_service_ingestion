
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import pandas as pd

logger = logging.getLogger(__name__)

class BaseLoader(ABC):
    """
    Abstract base class for all data loaders.
    Defines common interface and functionality for loading data into various storage systems.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize base loader with configuration.
        
        Args:
            config: Loader configuration
        """
        self.config = config
        self.connection = None
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the storage system.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the storage system."""
        pass
    
    @abstractmethod
    def create_table_if_not_exists(self, database: str, table_name: str, schema: Optional[Any] = None) -> bool:
        """
        Create table in the storage system if it doesn't exist.
        
        Args:
            database: Database or schema name
            table_name: Table name
            schema: Optional schema definition
            
        Returns:
            True if table exists or was created, False otherwise
        """
        pass
    
    @abstractmethod
    def load_data(self, 
                  data: pd.DataFrame, 
                  database: str, 
                  table_name: str,
                  mode: str = "append") -> int:
        """
        Load data into the storage system.
        
        Args:
            data: DataFrame to load
            database: Database or schema name
            table_name: Table name
            mode: Write mode ('append', 'overwrite', etc.)
            
        Returns:
            Number of rows written
        """
        pass
    
    def validate_data(self, data: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate data before loading.
        
        Args:
            data: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            True if valid, False otherwise
        """
        if data.empty:
            logger.warning("Empty DataFrame provided for loading")
            return False
            
        # Check if required columns exist
        missing_cols = [col for col in required_columns if col not in data.columns]
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
            
        return True
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()