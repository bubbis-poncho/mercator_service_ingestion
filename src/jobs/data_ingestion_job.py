import logging
import os
import yaml
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from ..connectors.connector_factory import ConnectorFactory
from ..transformers.transformer_factory import TransformerFactory
from ..loaders.loader_factory import LoaderFactory

logger = logging.getLogger(__name__)

class DataIngestionJob:
    """
    Generic data ingestion job that can be configured for any connector type.
    """
    
    def __init__(self, job_id: str, config: Dict[str, Any]):
        """
        Initialize data ingestion job with configuration.
        
        Args:
            job_id: Unique identifier for the job
            config: Job configuration containing:
                - enabled: Whether the job is enabled
                - cron_expression: Cron expression for scheduling
                - source: Source configuration
                - transformer: Transformer configuration
                - loader: Loader configuration
                - monitoring: Monitoring configuration
        """
        self.job_id = job_id
        self.config = config
        self.enabled = config.get("enabled", True)
        self.job_name = f"data_ingestion_{job_id}"
        
        # Initialize scheduler
        self.db_url = config.get("db_url", "sqlite:///jobs.sqlite")
        jobstores = {
            'default': SQLAlchemyJobStore(url=self.db_url)
        }
        self.scheduler = BackgroundScheduler(jobstores=jobstores)
        
        # Initialize components based on configuration
        self._init_components()
    
    def _init_components(self) -> None:
        """Initialize connector, transformer and loader components."""
        try:
            # Initialize source connector
            source_config = self.config.get("source", {})
            source_type = source_config.get("type")
            
            if not source_type:
                logger.error(f"Source type not specified for job {self.job_id}")
                raise ValueError(f"Source type not specified for job {self.job_id}")
                
            self.connector = ConnectorFactory.get_connector(source_type, source_config)
            
            if not self.connector:
                logger.error(f"Failed to create connector of type {source_type} for job {self.job_id}")
                raise ValueError(f"Failed to create connector of type {source_type}")
            
            # Initialize transformer
            transformer_config = self.config.get("transformer", {})
            transformer_type = transformer_config.get("type", "base")
            
            self.transformer = TransformerFactory.get_transformer(transformer_type, transformer_config)
            
            if not self.transformer:
                logger.error(f"Failed to create transformer of type {transformer_type} for job {self.job_id}")
                raise ValueError(f"Unsupported transformer type: {transformer_type}")
            
            # Initialize loader
            loader_config = self.config.get("loader", {})
            loader_type = loader_config.get("type", "iceberg")
            
            self.loader = LoaderFactory.get_loader(loader_type, loader_config)
            
            if not self.loader:
                logger.error(f"Failed to create loader of type {loader_type} for job {self.job_id}")
                raise ValueError(f"Unsupported loader type: {loader_type}")
                
        except Exception as e:
            logger.exception(f"Error initializing components for job {self.job_id}: {str(e)}")
            raise
    
    def start(self) -> None:
        """Start the scheduler with the configured cron expression."""
        if not self.enabled:
            logger.info(f"Job {self.job_id} is disabled, not starting")
            return
            
        # Get cron expression from config, default is daily at 3 AM
        cron_expression = self.config.get("cron_expression", "0 3 * * *")
        
        # Add job to scheduler
        self.scheduler.add_job(
            func=self.run_job,
            trigger=CronTrigger.from_crontab(cron_expression),
            id=self.job_name,
            replace_existing=True,
            misfire_grace_time=3600  # 1 hour grace time for misfired jobs
        )
        
        # Start scheduler
        self.scheduler.start()
        logger.info(f"Job {self.job_id} scheduled with cron expression: {cron_expression}")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info(f"Job {self.job_id} scheduler stopped")
    
    def run_now(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """
        Run the job immediately.
        
        Args:
            parameters: Optional parameters to override job configuration
        """
        self.scheduler.add_job(
            func=self.run_job,
            trigger="date",
            run_date=datetime.now(),
            id=f"{self.job_name}_manual_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            args=[parameters],
            replace_existing=False
        )
        logger.info(f"Scheduled immediate execution of job {self.job_id}")
    
    def run_job(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        """
        Execute the data ingestion job.
        
        Args:
            parameters: Optional parameters to override job configuration
        """
        start_time = datetime.now()
        job_run_id = f"{self.job_id}_{start_time.strftime('%Y%m%d%H%M%S')}"
        
        logger.info(f"Starting job {self.job_id} (run ID: {job_run_id})")
        
        try:
            # Merge run-time parameters with job configuration
            combined_config = self.config.copy()
            if parameters:
                # Deep merge of parameters into config
                self._deep_update(combined_config, parameters)
            
            # Get source data
            source_config = combined_config.get("source", {})
            source_type = source_config.get("type")
            
            logger.info(f"Extracting data from {source_type} source")
            
            # Different extraction methods based on connector type
            raw_data = None
            
            # Handle web scrapers
            if isinstance(self.connector, (WebScraper, FSAScraper)):
                # TODO: Handle specific scraper types
                raw_data = pd.DataFrame()  # Placeholder - implement scraper-specific logic
                
            # Handle API connectors
            elif isinstance(self.connector, APIConnector):
                endpoint = source_config.get("endpoint", "")
                params = source_config.get("params", {})
                data_key = source_config.get("data_key", "data")
                
                # Fetch data from API
                data = self.connector.fetch_all_pages(endpoint, params, data_key)
                raw_data = pd.DataFrame(data)
                
            # Handle database connectors
            elif isinstance(self.connector, DBConnector):
                if "query" in source_config:
                    # Use custom SQL query
                    query = source_config.get("query")
                    params = source_config.get("query_params", {})
                    raw_data = self.connector.execute_query_df(query, params)
                else:
                    # Extract from table
                    table = source_config.get("table")
                    schema = source_config.get("schema")
                    columns = source_config.get("columns")
                    where_clause = source_config.get("where_clause")
                    params = source_config.get("where_params", {})
                    
                    raw_data = self.connector.extract_data(
                        table_name=table,
                        schema=schema,
                        columns=columns,
                        where_clause=where_clause,
                        params=params
                    )
                    
            # Handle file connectors
            elif isinstance(self.connector, FileConnector):
                # Check if incremental processing
                if source_config.get("incremental", False):
                    last_processed_time = self._get_last_processed_time()
                    raw_data = self.connector.read_incremental(last_processed_time)
                else:
                    raw_data = self.connector.read_all_files()
            
            # Check if we got any data
            if raw_data is None or (isinstance(raw_data, pd.DataFrame) and raw_data.empty):
                logger.warning(f"No data extracted from source for job {self.job_id}")
                
                # Check if we should alert on zero records
                if combined_config.get("monitoring", {}).get("alert_on_zero_records", False):
                    self._send_alert(f"No data extracted from source for job {self.job_id}")
                    
                return
                
            logger.info(f"Extracted {len(raw_data)} rows from source")
            
            # Transform data
            logger.info(f"Transforming data using {self.transformer.__class__.__name__}")
            transformed_data = self.transformer.transform(raw_data)
            
            # Validate data
            if not self.transformer.validate(transformed_data):
                logger.error(f"Data validation failed for job {self.job_id}")
                
                if combined_config.get("monitoring", {}).get("alert_on_validation_failure", True):
                    self._send_alert(f"Data validation failed for job {self.job_id}")
                    
                return
                
            logger.info(f"Transformed data has {len(transformed_data)} rows and {len(transformed_data.columns)} columns")
            
            # Load data
            loader_config = combined_config.get("loader", {})
            database = loader_config.get("database", "default")
            table_name = loader_config.get("table", f"data_{self.job_id}")
            write_mode = loader_config.get("write_mode", "append")
            
            logger.info(f"Loading data to {database}.{table_name} using mode {write_mode}")
            
            # Ensure destination table exists
            self.loader.create_table_if_not_exists(database, table_name)
            
            # Load the data
            rows_loaded = self.loader.load_data(
                data=transformed_data,
                database=database,
                table_name=table_name,
                mode=write_mode
            )
            
            logger.info(f"Successfully loaded {rows_loaded} rows to {database}.{table_name}")
            
            # Update job metadata
            self._update_job_metadata(
                start_time=start_time,
                end_time=datetime.now(),
                rows_processed=len(raw_data),
                rows_loaded=rows_loaded,
                status="success"
            )
            
        except Exception as e:
            logger.exception(f"Error in job {self.job_id}: {str(e)}")
            
            # Update job metadata with failure status
            self._update_job_metadata(
                start_time=start_time,
                end_time=datetime.now(),
                status="failed",
                error_message=str(e)
            )
            
            # Send alert if configured
            if self.config.get("monitoring", {}).get("alert_on_failure", True):
                self._send_alert(f"Job {self.job_id} failed: {str(e)}")
                
        finally:
            # Close connections
            if hasattr(self.connector, 'disconnect') and callable(getattr(self.connector, 'disconnect')):
                self.connector.disconnect()
                
            if hasattr(self.loader, 'disconnect') and callable(getattr(self.loader, 'disconnect')):
                self.loader.disconnect()
    
    def _deep_update(self, d: Dict, u: Dict) -> Dict:
        """
        Deep update dictionary d with values from dictionary u.
        
        Args:
            d: Original dictionary
            u: Dictionary with updates
            
        Returns:
            Updated dictionary
        """
        for k, v in u.items():
            if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                self._deep_update(d[k], v)
            else:
                d[k] = v
        return d
    
    def _get_last_processed_time(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful job run.
        
        Returns:
            Datetime of last successful run or None
        """
        # This is a simplified implementation - in production you'd use a database
        metadata_file = f"metadata/job_{self.job_id}_metadata.yaml"
        
        try:
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    metadata = yaml.safe_load(f)
                    
                last_success = metadata.get("last_successful_run")
                if last_success:
                    return datetime.fromisoformat(last_success)
        except Exception as e:
            logger.error(f"Error reading job metadata: {str(e)}")
            
        # Default to 7 days ago if no metadata available
        return datetime.now() - timedelta(days=7)
    
    def _update_job_metadata(self, 
                           start_time: datetime,
                           end_time: datetime,
                           status: str,
                           rows_processed: int = 0,
                           rows_loaded: int = 0,
                           error_message: str = None) -> None:
        """
        Update job metadata with run information.
        
        Args:
            start_time: Job start time
            end_time: Job end time
            status: Job run status (success, failed)
            rows_processed: Number of rows processed
            rows_loaded: Number of rows loaded
            error_message: Error message if job failed
        """
        # This is a simplified implementation - in production you'd use a database
        metadata_dir = "metadata"
        os.makedirs(metadata_dir, exist_ok=True)
        
        metadata_file = f"{metadata_dir}/job_{self.job_id}_metadata.yaml"
        
        # Load existing metadata if available
        metadata = {}
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = yaml.safe_load(f) or {}
            except Exception as e:
                logger.error(f"Error reading metadata file: {str(e)}")
        
        # Update run history
        runs = metadata.get("runs", [])
        run_info = {
            "run_id": f"{self.job_id}_{start_time.strftime('%Y%m%d%H%M%S')}",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
            "status": status,
            "rows_processed": rows_processed,
            "rows_loaded": rows_loaded
        }
        
        if error_message:
            run_info["error_message"] = error_message
            
        runs.append(run_info)
        metadata["runs"] = runs[-10:]  # Keep only last 10 runs
        
        # Update last run info
        metadata["last_run"] = start_time.isoformat()
        if status == "success":
            metadata["last_successful_run"] = end_time.isoformat()
        
        # Write updated metadata
        try:
            with open(metadata_file, 'w') as f:
                yaml.dump(metadata, f)
        except Exception as e:
            logger.error(f"Error writing metadata file: {str(e)}")
    
    def _send_alert(self, message: str) -> None:
        """
        Send alert notification.
        
        Args:
            message: Alert message
        """
        monitoring_config = self.config.get("monitoring", {})
        notification_channels = monitoring_config.get("notification_channels", [])
        
        logger.warning(f"ALERT: {message}")
        
        # This is a placeholder - implement actual notification logic
        if "slack" in notification_channels:
            logger.info(f"Would send Slack alert: {message}")
            
        if "email" in notification_channels:
            recipients = []
            if monitoring_config.get("notify_owners", False) and "metadata" in self.config:
                owner_email = self.config.get("metadata", {}).get("contact_email")
                if owner_email:
                    recipients.append(owner_email)
            
            logger.info(f"Would send email alert to {recipients}: {message}")