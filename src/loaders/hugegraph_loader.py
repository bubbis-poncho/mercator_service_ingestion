# src/loaders/hugegraph_loader.py
import logging
import os
import json
from typing import Dict, List, Optional, Any, Union, Tuple
import pandas as pd
import requests
from datetime import datetime
import hashlib
from urllib.parse import urljoin
import time

from .base_loader import BaseLoader

# Define constants for Hugegraph data types
HUGEGRAPH_TYPES = {
    "TEXT": "TEXT",
    "STRING": "TEXT",
    "str": "TEXT",
    "BOOLEAN": "BOOLEAN",
    "bool": "BOOLEAN",
    "BYTE": "BYTE",
    "INT": "INT",
    "INTEGER": "INT",
    "int": "INT",
    "LONG": "LONG",
    "FLOAT": "FLOAT",
    "float": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DATE": "DATE",
    "datetime": "DATE",
    "UUID": "UUID"
}

logger = logging.getLogger(__name__)

class HugegraphLoader(BaseLoader):
    """
    Loader for writing data to Hugegraph graph database.
    Supports creating vertices, edges, and managing schemas.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Hugegraph loader.
        
        Args:
            config: Loader configuration containing:
                - host: Hugegraph server host (required)
                - port: Hugegraph server port (default: 8080)
                - graph: Hugegraph graph name (required)
                - username: Username for authentication (optional)
                - password: Password for authentication (optional)
                - batch_size: Number of records per batch (default: 500)
                - timeout: Request timeout in seconds (default: 30)
                - auto_create_schema: Whether to automatically create schema elements (default: True)
                - vertex_mappings: Mappings for vertex types
                - edge_mappings: Mappings for edge types
        """
        super().__init__(config)
        
        # Required parameters
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 8080)
        self.graph = config.get("graph")
        
        # Optional parameters
        self.username = config.get("username")
        self.password = config.get("password")
        self.batch_size = config.get("batch_size", 500)
        self.timeout = config.get("timeout", 30)
        self.auto_create_schema = config.get("auto_create_schema", True)
        
        # Schema mappings
        self.vertex_mappings = config.get("vertex_mappings", {})
        self.edge_mappings = config.get("edge_mappings", {})
        
        # Session for API requests
        self.session = None
        self.base_url = None
        
        # Schema cache
        self.vertex_labels = set()
        self.edge_labels = set()
        self.property_keys = set()
        
        # Validate required parameters
        if not self.graph:
            raise ValueError("Missing required parameter: graph")
    
    def connect(self) -> bool:
        """
        Establish connection to Hugegraph.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.session is not None:
                return True
            
            # Create base URL
            self.base_url = f"http://{self.host}:{self.port}"
            
            # Create session
            self.session = requests.Session()
            
            # Set up authentication if provided
            if self.username and self.password:
                self.session.auth = (self.username, self.password)
            
            # Test connection
            response = self.session.get(
                urljoin(self.base_url, f"graphs/{self.graph}"),
                timeout=self.timeout
            )
            response.raise_for_status()
            
            logger.info(f"Connected to Hugegraph at {self.host}:{self.port}, graph: {self.graph}")
            
            # Load existing schema
            self._load_schema()
            
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Hugegraph: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close Hugegraph connection."""
        if self.session is not None:
            self.session.close()
            self.session = None
            logger.info("Hugegraph connection closed")
    
    def _load_schema(self) -> None:
        """Load existing schema elements from Hugegraph."""
        try:
            # Load vertex labels
            response = self.session.get(
                urljoin(self.base_url, f"graphs/{self.graph}/schema/vertexlabels"),
                timeout=self.timeout
            )
            response.raise_for_status()
            vertex_labels = response.json().get("vertexlabels", [])
            self.vertex_labels = {vl["name"] for vl in vertex_labels}
            
            # Load edge labels
            response = self.session.get(
                urljoin(self.base_url, f"graphs/{self.graph}/schema/edgelabels"),
                timeout=self.timeout
            )
            response.raise_for_status()
            edge_labels = response.json().get("edgelabels", [])
            self.edge_labels = {el["name"] for el in edge_labels}
            
            # Load property keys
            response = self.session.get(
                urljoin(self.base_url, f"graphs/{self.graph}/schema/propertykeys"),
                timeout=self.timeout
            )
            response.raise_for_status()
            property_keys = response.json().get("propertykeys", [])
            self.property_keys = {pk["name"] for pk in property_keys}
            
            logger.info(f"Loaded schema: {len(self.vertex_labels)} vertex labels, " +
                        f"{len(self.edge_labels)} edge labels, {len(self.property_keys)} property keys")
        except Exception as e:
            logger.error(f"Error loading schema: {str(e)}")
            raise
    
    def create_table_if_not_exists(self,
                                 database: str,
                                 table_name: str,
                                 schema: Optional[Dict] = None) -> bool:
        """
        Create Hugegraph vertex or edge label if it doesn't exist.
        
        In Hugegraph context:
        - database parameter is used as vertex or edge label group/category
        - table_name parameter is used as vertex or edge label name
        
        Args:
            database: Category or group name
            table_name: Label name
            schema: Schema definition
            
        Returns:
            True if label exists or was created, False otherwise
        """
        # For Hugegraph, we'll interpret this as creating vertex or edge labels
        # Determine if this is a vertex or edge based on schema
        try:
            if not self.connect():
                return False
            
            # Construct full label name
            label_name = f"{database}_{table_name}" if database else table_name
            
            # Determine if this is a vertex or edge
            is_edge = False
            if schema and "edge" in schema:
                is_edge = schema.get("edge", False)
            
            # Check if label already exists
            if is_edge and label_name in self.edge_labels:
                logger.info(f"Edge label {label_name} already exists")
                return True
            elif not is_edge and label_name in self.vertex_labels:
                logger.info(f"Vertex label {label_name} already exists")
                return True
            
            # If auto-create is not enabled, return False
            if not self.auto_create_schema:
                logger.warning(f"Label {label_name} does not exist and auto-create is disabled")
                return False
            
            # Create property keys if needed
            if schema and "properties" in schema:
                for prop_name, prop_type in schema["properties"].items():
                    self._ensure_property_key(prop_name, prop_type)
            
            # Create the label
            if is_edge:
                self._create_edge_label(label_name, schema)
            else:
                self._create_vertex_label(label_name, schema)
            
            logger.info(f"Created {'edge' if is_edge else 'vertex'} label: {label_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating label {database}_{table_name}: {str(e)}")
            return False
    
    def _ensure_property_key(self, name: str, data_type: str) -> bool:
        """
        Ensure property key exists, create if it doesn't.
        
        Args:
            name: Property key name
            data_type: Property data type
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if property key already exists
            if name in self.property_keys:
                return True
            
            # Map data type to Hugegraph data type
            hugegraph_type = self._map_data_type(data_type)
            
            # Create property key
            payload = {
                "name": name,
                "data_type": hugegraph_type,
                "cardinality": "SINGLE"
            }
            
            response = self.session.post(
                urljoin(self.base_url, f"graphs/{self.graph}/schema/propertykeys"),
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Add to cache
            self.property_keys.add(name)
            
            return True
        except Exception as e:
            logger.error(f"Error creating property key {name}: {str(e)}")
            return False
    
    def _map_data_type(self, data_type: str) -> str:
        """
        Map Python/Pandas data type to Hugegraph data type.
        
        Args:
            data_type: Source data type
            
        Returns:
            Hugegraph data type
        """
        data_type = str(data_type).upper()
        return HUGEGRAPH_TYPES.get(data_type, "TEXT")
    
    def _create_vertex_label(self, name: str, schema: Dict) -> bool:
        """
        Create a vertex label.
        
        Args:
            name: Vertex label name
            schema: Schema definition
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare properties list
            properties = list(schema.get("properties", {}).keys())
            
            # Determine ID strategy
            id_strategy = schema.get("id_strategy", "PRIMARY_KEY")
            
            # Set up primary keys if using PRIMARY_KEY strategy
            primary_keys = []
            if id_strategy == "PRIMARY_KEY":
                primary_keys = schema.get("primary_keys", [])
                if not primary_keys:
                    logger.warning(f"No primary keys specified for vertex label {name} with PRIMARY_KEY strategy")
                    # Fallback to AUTOMATIC
                    id_strategy = "AUTOMATIC"
            
            # Create payload
            payload = {
                "name": name,
                "id_strategy": id_strategy,
                "properties": properties
            }
            
            if primary_keys:
                payload["primary_keys"] = primary_keys
            
            # Send request
            response = self.session.post(
                urljoin(self.base_url, f"graphs/{self.graph}/schema/vertexlabels"),
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Add to cache
            self.vertex_labels.add(name)
            
            return True
        except Exception as e:
            logger.error(f"Error creating vertex label {name}: {str(e)}")
            return False
    
    def _create_edge_label(self, name: str, schema: Dict) -> bool:
        """
        Create an edge label.
        
        Args:
            name: Edge label name
            schema: Schema definition
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get source and target vertex labels
            source_label = schema.get("source_label")
            target_label = schema.get("target_label")
            
            if not source_label or not target_label:
                logger.error(f"Source and target labels are required for edge label {name}")
                return False
            
            # Ensure source and target vertex labels exist
            if source_label not in self.vertex_labels:
                logger.error(f"Source vertex label {source_label} does not exist")
                return False
            
            if target_label not in self.vertex_labels:
                logger.error(f"Target vertex label {target_label} does not exist")
                return False
            
            # Prepare properties list
            properties = list(schema.get("properties", {}).keys())
            
            # Determine frequency (allows multiple edges between same vertices)
            frequency = schema.get("frequency", "SINGLE")
            
            # Create payload
            payload = {
                "name": name,
                "source_label": source_label,
                "target_label": target_label,
                "frequency": frequency,
                "properties": properties
            }
            
            # Sort keys (optional)
            sort_keys = schema.get("sort_keys", [])
            if sort_keys:
                payload["sort_keys"] = sort_keys
            
            # Send request
            response = self.session.post(
                urljoin(self.base_url, f"graphs/{self.graph}/schema/edgelabels"),
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Add to cache
            self.edge_labels.add(name)
            
            return True
        except Exception as e:
            logger.error(f"Error creating edge label {name}: {str(e)}")
            return False