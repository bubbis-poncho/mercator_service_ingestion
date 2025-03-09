
from typing import Dict, Any, Optional, Type

from .base_transformer import BaseTransformer
from .source_transformers.fsa_transformer import FSADataTransformer

class TransformerFactory:
    """
    Factory class for creating data transformer instances based on type.
    """
    
    # Register all transformer types
    _transformers = {
        'fsa': FSADataTransformer,
        'swedish_fsa': FSADataTransformer,  # alias
    }
    
    @classmethod
    def get_transformer(cls, transformer_type: str, config: Optional[Dict[str, Any]] = None) -> Optional[BaseTransformer]:
        """
        Get a transformer instance of the specified type.
        
        Args:
            transformer_type: Type of transformer ('fsa', etc.)
            config: Optional transformer configuration
            
        Returns:
            Instance of specified transformer type, or None if type not found
        """
        transformer_class = cls._transformers.get(transformer_type.lower())
        
        if transformer_class:
            return transformer_class(config)
        else:
            return None
    
    @classmethod
    def register_transformer(cls, transformer_type: str, transformer_class: Type[BaseTransformer]) -> None:
        """
        Register a new transformer type.
        
        Args:
            transformer_type: Type name for the transformer
            transformer_class: Transformer class to register
        """
        cls._transformers[transformer_type.lower()] = transformer_class