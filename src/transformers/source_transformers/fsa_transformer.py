import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Union

from ..base_transformer import BaseTransformer

logger = logging.getLogger(__name__)

class FSADataTransformer(BaseTransformer):
    """
    Transformer for Swedish FSA insider trading data.
    Transforms raw scraped data into a structured format suitable for storage.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize FSA data transformer.
        
        Args:
            config: Optional configuration dict
        """
        super().__init__(config)
        self.date_columns = ['Publiceringsdatum', 'Transaktionsdatum']
        self.numeric_columns = ['Volym', 'Pris', 'Volymsenhet']
        self.boolean_columns = ['Ja']
        
        # Column mapping from Swedish to English
        self.column_mapping = {
            'Publiceringsdatum': 'publication_date',
            'Emittent': 'issuer',
            'Person i ledande ställning': 'person_in_leading_position',
            'Befattning': 'position',
            'Närstående': 'related_party',
            'Karaktär': 'transaction_type',
            'Instrumentnamn': 'instrument_name',
            'Instrumenttyp': 'instrument_type',
            'ISIN': 'isin',
            'Transaktionsdatum': 'transaction_date',
            'Volym': 'volume',
            'Volymsenhet': 'volume_unit',
            'Pris': 'price',
            'Valuta': 'currency',
            'Status': 'status',
            'Detaljer': 'details',
            'details_link': 'details_link'
        }
    
    def transform(self, data: Union[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform raw scraped data into a structured format.
        
        Args:
            data: Raw data as DataFrame, list of dictionaries, or dictionary
            
        Returns:
            Transformed DataFrame
        """
        # Convert to DataFrame if not already
        if not isinstance(data, pd.DataFrame):
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                logger.error(f"Unsupported data type: {type(data)}")
                return pd.DataFrame()
        else:
            df = data.copy()
            
        if df.empty:
            logger.warning("Empty DataFrame provided to transformer")
            return df
        
        # Rename columns to English where they exist
        existing_columns = set(df.columns).intersection(set(self.column_mapping.keys()))
        rename_dict = {col: self.column_mapping[col] for col in existing_columns}
        df = df.rename(columns=rename_dict)
        
        # Convert data types using base class utility
        english_date_cols = [self.column_mapping.get(col, col) for col in self.date_columns
                             if col in existing_columns]
        english_numeric_cols = [self.column_mapping.get(col, col) for col in self.numeric_columns 
                               if col in existing_columns]
        english_boolean_cols = [self.column_mapping.get(col, col) for col in self.boolean_columns
                               if col in existing_columns]
        
        df = self.convert_data_types(
            df,
            date_columns=english_date_cols,
            numeric_columns=english_numeric_cols,
            boolean_columns=english_boolean_cols
        )
        
        # Add metadata columns
        df['source'] = 'Swedish FSA'
        df['ingestion_timestamp'] = datetime.now()
        
        return df
    
    def validate(self, data: pd.DataFrame) -> bool:
        """
        Validate the transformed data.
        
        Args:
            data: Transformed data
            
        Returns:
            True if valid, False otherwise
        """
        # Check if required columns exist
        required_cols = ['issuer', 'transaction_date', 'volume', 'price']
        missing_cols = [col for col in required_cols if col not in data.columns]
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
            
        # Check for missing values in critical columns
        critical_cols = ['issuer', 'transaction_date']
        for col in critical_cols:
            if col in data.columns and data[col].isna().any():
                null_count = data[col].isna().sum()
                logger.warning(f"Column {col} has {null_count} null values")
                
        # Check data types
        expected_types = {
            'transaction_date': 'datetime64[ns]',
            'volume': 'float64',
            'price': 'float64'
        }
        
        for col, expected_type in expected_types.items():
            if col in data.columns and not pd.api.types.is_dtype_equal(data[col].dtype, expected_type):
                logger.warning(f"Column {col} has type {data[col].dtype}, expected {expected_type}")
        
        return True