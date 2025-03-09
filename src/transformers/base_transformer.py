
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
import pandas as pd

logger = logging.getLogger(__name__)

class BaseTransformer(ABC):
    """
    Abstract base class for all data transformers.
    Defines common interface and functionality for transforming scraped data.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize base transformer with optional configuration.
        
        Args:
            config: Optional transformer configuration
        """
        self.config = config or {}
    
    @abstractmethod
    def transform(self, data: Union[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform raw data into a structured format.
        
        Args:
            data: Raw data as DataFrame, list of dictionaries, or dictionary
            
        Returns:
            Transformed DataFrame
        """
        pass
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> bool:
        """
        Validate the transformed data.
        
        Args:
            data: Transformed data
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize column names (utility method for subclasses).
        
        Args:
            df: DataFrame with column names to clean
            
        Returns:
            DataFrame with cleaned column names
        """
        # Create a copy to avoid modifying the original
        cleaned_df = df.copy()
        
        # Clean column names: lowercase, replace spaces with underscores
        cleaned_df.columns = [
            col.lower().replace(' ', '_').replace('-', '_')
            for col in cleaned_df.columns
        ]
        
        return cleaned_df
    
    def convert_data_types(self, 
                          df: pd.DataFrame, 
                          date_columns: List[str] = None, 
                          numeric_columns: List[str] = None,
                          boolean_columns: List[str] = None) -> pd.DataFrame:
        """
        Convert columns to appropriate data types (utility method for subclasses).
        
        Args:
            df: DataFrame to convert
            date_columns: List of column names to convert to datetime
            numeric_columns: List of column names to convert to numeric
            boolean_columns: List of column names to convert to boolean
            
        Returns:
            DataFrame with converted data types
        """
        # Create a copy to avoid modifying the original
        converted_df = df.copy()
        
        # Convert date columns
        if date_columns:
            for col in date_columns:
                if col in converted_df.columns:
                    converted_df[col] = pd.to_datetime(converted_df[col], errors='coerce')
        
        # Convert numeric columns
        if numeric_columns:
            for col in numeric_columns:
                if col in converted_df.columns:
                    # Replace commas with dots for decimal points
                    if converted_df[col].dtype == 'object':
                        converted_df[col] = converted_df[col].str.replace(',', '.', regex=False)
                    converted_df[col] = pd.to_numeric(converted_df[col], errors='coerce')
        
        # Convert boolean columns
        if boolean_columns:
            for col in boolean_columns:
                if col in converted_df.columns:
                    # Map common boolean values
                    bool_map = {
                        'yes': True, 'no': False,
                        'true': True, 'false': False,
                        'y': True, 'n': False,
                        '1': True, '0': False,
                        'ja': True, 'nej': False,  # Swedish
                    }
                    
                    # Apply mapping for string values (case-insensitive)
                    if converted_df[col].dtype == 'object':
                        converted_df[col] = converted_df[col].str.lower().map(bool_map)
                    
        return converted_df