import logging
import os
import glob
import pandas as pd
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime

logger = logging.getLogger(__name__)

class FileConnector:
    """Base file connector class with common functionality for reading various file formats."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize file connector with configuration.
        
        Args:
            config: File connector configuration containing:
                - source_path: Directory or file path
                - file_pattern: Optional glob pattern for matching files
                - file_type: Type of file (csv, json, excel, xml, parquet, avro, etc.)
                - encoding: Character encoding (default: utf-8)
                - file_options: Format-specific options
        """
        self.config = config
        self.source_path = config.get("source_path", "")
        self.file_pattern = config.get("file_pattern", "*")
        self.file_type = config.get("file_type", "").lower()
        self.encoding = config.get("encoding", "utf-8")
        self.file_options = config.get("file_options", {})
        
    def list_files(self) -> List[str]:
        """
        List all files matching the configured pattern.
        
        Returns:
            List of file paths
        """
        if os.path.isfile(self.source_path):
            return [self.source_path]
        elif os.path.isdir(self.source_path):
            pattern = os.path.join(self.source_path, self.file_pattern)
            return sorted(glob.glob(pattern))
        else:
            # Try direct glob pattern
            return sorted(glob.glob(self.source_path))
    
    def read_file(self, file_path: str) -> pd.DataFrame:
        """
        Read a file into a pandas DataFrame.
        
        Args:
            file_path: Path to the file
            
        Returns:
            DataFrame containing the file data
        """
        try:
            # Auto-detect file type if not specified
            file_type = self.file_type
            if not file_type:
                file_ext = os.path.splitext(file_path)[1].lower().lstrip('.')
                file_type = file_ext
            
            logger.info(f"Reading {file_type} file: {file_path}")
            
            if file_type in ('csv', 'tsv', 'txt'):
                # Set default delimiter based on file type
                if 'delimiter' not in self.file_options:
                    if file_type == 'tsv':
                        self.file_options['delimiter'] = '\t'
                    elif file_type == 'csv':
                        self.file_options['delimiter'] = ','
                
                # Common defaults for CSV files
                options = {
                    'encoding': self.encoding,
                    'parse_dates': True,
                    'infer_datetime_format': True,
                    'low_memory': False
                }
                options.update(self.file_options)
                
                return pd.read_csv(file_path, **options)
                
            elif file_type in ('xls', 'xlsx', 'excel'):
                sheet_name = self.file_options.get('sheet_name', 0)
                header = self.file_options.get('header', 0)
                skiprows = self.file_options.get('skiprows')
                
                return pd.read_excel(file_path, sheet_name=sheet_name, header=header, skiprows=skiprows)
                
            elif file_type == 'json':
                orient = self.file_options.get('orient')
                lines = self.file_options.get('lines', False)
                
                if lines:
                    # JSON Lines format (one JSON object per line)
                    return pd.read_json(file_path, lines=True, orient=orient)
                else:
                    # Regular JSON
                    with open(file_path, 'r', encoding=self.encoding) as f:
                        data = json.load(f)
                    
                    # Handle different JSON structures
                    if isinstance(data, list):
                        if all(isinstance(item, dict) for item in data):
                            return pd.DataFrame(data)
                        else:
                            return pd.DataFrame({"data": data})
                    elif isinstance(data, dict):
                        # If it's a nested structure, try to find the main data array
                        for key, value in data.items():
                            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                                return pd.DataFrame(value)
                        
                        # If no clear data array, convert the dict to a single-row DataFrame
                        return pd.DataFrame([data])
                    else:
                        return pd.DataFrame({"data": [data]})
                
            elif file_type == 'xml':
                xpath = self.file_options.get('xpath', '/root/*')
                
                # Parse XML
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                # Extract elements using XPath
                elements = root.findall(xpath)
                
                # Convert elements to list of dicts
                data = []
                for element in elements:
                    item = {}
                    for child in element:
                        item[child.tag] = child.text
                    
                    # Add attributes if any
                    for key, value in element.attrib.items():
                        item[f"{element.tag}_{key}"] = value
                        
                    data.append(item)
                
                return pd.DataFrame(data)
                
            elif file_type == 'parquet':
                return pd.read_parquet(file_path)
                
            elif file_type == 'avro':
                try:
                    import fastavro
                except ImportError:
                    logger.error("fastavro library not installed. Install with 'pip install fastavro'")
                    raise
                
                with open(file_path, 'rb') as f:
                    reader = fastavro.reader(f)
                    records = [r for r in reader]
                    
                return pd.DataFrame(records)
                
            elif file_type == 'orc':
                try:
                    import pyorc
                except ImportError:
                    logger.error("pyorc library not installed. Install with 'pip install pyorc'")
                    raise
                
                with open(file_path, 'rb') as f:
                    reader = pyorc.Reader(f)
                    records = [row for row in reader]
                    
                return pd.DataFrame(records, columns=reader.schema.fields.keys())
                
            else:
                logger.error(f"Unsupported file type: {file_type}")
                raise ValueError(f"Unsupported file type: {file_type}")
                
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
    def read_all_files(self) -> pd.DataFrame:
        """
        Read all matching files and combine into a single DataFrame.
        
        Returns:
            Combined DataFrame containing data from all files
        """
        files = self.list_files()
        
        if not files:
            logger.warning(f"No files found matching pattern: {self.source_path}/{self.file_pattern}")
            return pd.DataFrame()
            
        logger.info(f"Found {len(files)} files to process")
        
        all_data = []
        for file_path in files:
            try:
                df = self.read_file(file_path)
                
                # Add metadata columns if requested
                if self.config.get("add_file_metadata", False):
                    df['source_file'] = os.path.basename(file_path)
                    df['source_path'] = os.path.dirname(file_path)
                    df['ingestion_time'] = datetime.now()
                
                all_data.append(df)
                logger.info(f"Successfully read {len(df)} rows from {file_path}")
                
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                if not self.config.get("skip_errors", False):
                    raise
        
        if not all_data:
            logger.warning("No data read from any files")
            return pd.DataFrame()
            
        # Combine all DataFrames
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Combined data contains {len(combined_df)} rows")
        
        return combined_df
    
    def read_incremental(self, 
                        last_processed_time: datetime = None, 
                        time_field: str = 'mtime') -> pd.DataFrame:
        """
        Read only files that have been modified since the last processed time.
        
        Args:
            last_processed_time: Timestamp of last processing
            time_field: Field to use for comparison ('mtime', 'ctime', or 'atime')
            
        Returns:
            DataFrame containing data from new/modified files
        """
        if not last_processed_time:
            return self.read_all_files()
            
        files = self.list_files()
        new_files = []
        
        for file_path in files:
            file_time = None
            
            if time_field == 'mtime':
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            elif time_field == 'ctime':
                file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            elif time_field == 'atime':
                file_time = datetime.fromtimestamp(os.path.getatime(file_path))
                
            if file_time and file_time > last_processed_time:
                new_files.append(file_path)
        
        if not new_files:
            logger.info(f"No new or modified files since {last_processed_time}")
            return pd.DataFrame()
            
        logger.info(f"Found {len(new_files)} new or modified files since {last_processed_time}")
        
        all_data = []
        for file_path in new_files:
            try:
                df = self.read_file(file_path)
                
                # Add metadata columns if requested
                if self.config.get("add_file_metadata", False):
                    df['source_file'] = os.path.basename(file_path)
                    df['source_path'] = os.path.dirname(file_path)
                    df['ingestion_time'] = datetime.now()
                
                all_data.append(df)
                logger.info(f"Successfully read {len(df)} rows from {file_path}")
                
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                if not self.config.get("skip_errors", False):
                    raise
        
        if not all_data:
            logger.warning("No data read from any new files")
            return pd.DataFrame()
            
        # Combine all DataFrames
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Combined data contains {len(combined_df)} rows")
        
        return combined_df
    
    def get_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Get metadata for a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary of file metadata
        """
        try:
            stat = os.stat(file_path)
            
            return {
                'filename': os.path.basename(file_path),
                'path': os.path.dirname(file_path),
                'size': stat.st_size,
                'created': datetime.fromtimestamp(stat.st_ctime),
                'modified': datetime.fromtimestamp(stat.st_mtime),
                'accessed': datetime.fromtimestamp(stat.st_atime)
            }
        except Exception as e:
            logger.error(f"Error getting metadata for {file_path}: {str(e)}")
            raise

