
from typing import Dict, Any, Optional, Type

from .base_loader import BaseLoader
from .iceberg_loader import IcebergLoader
from .postgres_loader import PostgresLoader
from .delta_lake_loader import DeltaLakeLoader

class LoaderFactory:
    """
    Factory class for creating data loader instances based on type.
    """
    
    # Register all loader types
    _loaders = {
        'iceberg': IcebergLoader,
        'postgres': PostgresLoader,
        'delta': DeltaLakeLoader,
    }
    
    @classmethod
    def get_loader(cls, loader_type: str, config: Dict[str, Any]) -> Optional[BaseLoader]:
        """
        Get a loader instance of the specified type.
        
        Args:
            loader_type: Type of loader ('iceberg', 'postgres', 'delta')
            config: Loader configuration
            
        Returns:
            Instance of specified loader type, or None if type not found
        """
        loader_class = cls._loaders.get(loader_type.lower())
        
        if loader_class:
            return loader_class(config)
        else:
            return None
    
    @classmethod
    def register_loader(cls, loader_type: str, loader_class: Type[BaseLoader]) -> None:
        """
        Register a new loader type.
        
        Args:
            loader_type: Type name for the loader
            loader_class: Loader class to register
        """
        cls._loaders[loader_type.lower()] = loader_class