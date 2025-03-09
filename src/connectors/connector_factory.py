import logging
from typing import Dict, Any, Optional, Type, Union

# Import all connector types
from .web_scraper import WebScraper
from .web_scraper.parsers.fsa_parser import FSAScraper
from .api_connector import APIConnector
from .db_connector import DBConnector
from .file_connector import FileConnector

logger = logging.getLogger(__name__)

class ConnectorFactory:
    """
    Factory class for creating connector instances based on type.
    """
    
    # Register all connector types
    _connectors = {
        # Web scrapers
        'web_scraper': WebScraper,
        'fsa_scraper': FSAScraper,
        
        # API connectors
        'api': APIConnector,
        'rest_api': APIConnector,
        
        # Database connectors
        'database': DBConnector,
        'db': DBConnector,
        'postgresql': DBConnector,
        'mysql': DBConnector,
        'sqlite': DBConnector,
        'oracle': DBConnector,
        'mssql': DBConnector,
        
        # File connectors
        'file': FileConnector,
        'csv': FileConnector,
        'excel': FileConnector,
        'json': FileConnector,
        'xml': FileConnector,
        'parquet': FileConnector,
        'avro': FileConnector,
        'orc': FileConnector,
    }
    
    @classmethod
    def get_connector(cls, 
                     connector_type: str, 
                     config: Dict[str, Any]) -> Optional[Union[WebScraper, APIConnector, DBConnector, FileConnector]]:
        """
        Get a connector instance of the specified type.
        
        Args:
            connector_type: Type of connector
            config: Connector configuration
            
        Returns:
            Instance of specified connector type, or None if type not found
        """
        connector_class = cls._connectors.get(connector_type.lower())
        
        if connector_class:
            # Special handling for database connectors
            if connector_class == DBConnector and connector_type.lower() not in ('database', 'db'):
                # Set db_type based on connector_type if not already set
                if 'db_type' not in config:
                    config['db_type'] = connector_type.lower()
            
            # Special handling for file connectors
            if connector_class == FileConnector and connector_type.lower() != 'file':
                # Set file_type based on connector_type if not already set
                if 'file_type' not in config:
                    config['file_type'] = connector_type.lower()
            
            return connector_class(config)
        else:
            logger.error(f"Unsupported connector type: {connector_type}")
            return None
    
    @classmethod
    def register_connector(cls, connector_type: str, connector_class: Type) -> None:
        """
        Register a new connector type.
        
        Args:
            connector_type: Type name for the connector
            connector_class: Connector class to register
        """
        cls._connectors[connector_type.lower()] = connector_class

# src/connectors/__init__.py
from .connector_factory import ConnectorFactory
from .web_scraper import WebScraper
from .api_connector import APIConnector
from .db_connector import DBConnector
from .file_connector import FileConnector

__all__ = ['ConnectorFactory', 'WebScraper', 'APIConnector', 'DBConnector', 'FileConnector']