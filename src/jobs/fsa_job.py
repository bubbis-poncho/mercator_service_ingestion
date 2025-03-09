import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from ..connectors.web_scraper import FSAScraper, ScraperConfig
from ..transformers.transformer_factory import TransformerFactory
from ..loaders.loader_factory import LoaderFactory

logger = logging.getLogger(__name__)

class FSAJobScheduler:
    """
    Scheduler for FSA insider trading data collection job.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize scheduler with modular components.
        
        Args:
            config: Job configuration
        """
        self.config = config
        self.job_name = "fsa_insider_trading_job"
        
        # Initialize scraper component
        self.scraper = FSAScraper(ScraperConfig(
            user_agent=config.get("user_agent", "Mozilla/5.0"),
            requests_per_minute=config.get("requests_per_minute", 10),
            timeout=config.get("timeout", 30),
            random_delay=config.get("random_delay", True)
        ))
        
        # Initialize transformer through factory
        transformer_type = config.get("transformer_type", "fsa")
        transformer_config = config.get("transformer_config", {})
        self.transformer = TransformerFactory.get_transformer(transformer_type, transformer_config)
        
        if not self.transformer:
            logger.error(f"Failed to create transformer of type: {transformer_type}")
            raise ValueError(f"Unsupported transformer type: {transformer_type}")
        
        # Initialize loader through factory
        loader_type = config.get("loader_type", "iceberg")
        loader_config = config.get("loader_config", {})
        
        # Set default loader config if not provided
        if loader_type == "iceberg" and not loader_config:
            loader_config = {
                "spark_config": {
                    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0",
                    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                    "spark.sql.catalog.spark_catalog.type": "hive",
                    "spark.sql.warehouse.dir": config.get("warehouse_dir", "/warehouse/tablespace/external/hive"),
                    "spark.driver.memory": config.get("spark_driver_memory", "4g"),
                    "spark.executor.memory": config.get("spark_executor_memory", "4g")
                },
                "catalog_name": config.get("catalog_name", "hive_prod"),
                "warehouse_location": config.get("warehouse_location")
            }
        
        self.loader = LoaderFactory.get_loader(loader_type, loader_config)
        
        if not self.loader:
            logger.error(f"Failed to create loader of type: {loader_type}")
            raise ValueError(f"Unsupported loader type: {loader_type}")
        
        # Initialize scheduler
        jobstores = {
            'default': SQLAlchemyJobStore(url=config.get("db_url", "sqlite:///jobs.sqlite"))
        }
        
        self.scheduler = BackgroundScheduler(jobstores=jobstores)
    
    def start(self) -> None:
        """Start the scheduler."""
        # Schedule FSA job with cron expression from config, default is daily at 1 AM
        cron_expression = self.config.get("cron_expression", "0 1 * * *")
        
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
        logger.info(f"Scheduler started with cron expression: {cron_expression}")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")
    
    def run_now(self) -> None:
        """Run the FSA job immediately."""
        self.scheduler.add_job(
            func=self.run_job,
            trigger="date",
            run_date=datetime.now(),
            id=f"{self.job_name}_manual_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            replace_existing=False
        )
        logger.info("Scheduled immediate job execution")
    
    def run_job(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> None:
        """
        Execute the FSA data collection job using modular components.
        
        Args:
            start_date: Optional start date for data collection
            end_date: Optional end date for data collection
        """
        logger.info("Starting FSA insider trading data collection job")
        
        try:
            # Default to collecting last 7 days if not specified
            if not start_date:
                start_date = datetime.now() - timedelta(days=7)
            if not end_date:
                end_date = datetime.now()
                
            # Step 1: Scrape data
            logger.info(f"Scraping data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            raw_data = self.scraper.search_insider_trading(
                start_date=start_date,
                end_date=end_date
            )
            
            if raw_data.empty:
                logger.warning("No data scraped, job completed")
                return
                
            logger.info(f"Scraped {len(raw_data)} records")
            
            # Step 2: Transform data
            transformed_data = self.transformer.transform(raw_data)
            
            # Validate data
            if not self.transformer.validate(transformed_data):
                logger.error("Data validation failed")
                # Could implement notification or alerting here
                return
                
            logger.info(f"Transformed data has {len(transformed_data)} records and {len(transformed_data.columns)} columns")
            
            # Step 3: Load data
            database = self.config.get("database", "financial_data")
            table = self.config.get("table", "insider_trading")
            
            # If using Iceberg loader, create table with specific schema
            if isinstance(self.loader.__class__.__name__, str) and 'IcebergLoader' in self.loader.__class__.__name__:
                # Use Iceberg-specific method if available
                if hasattr(self.loader, 'create_fsa_insider_trading_table'):
                    self.loader.create_fsa_insider_trading_table(database, table)
            
            # Load data
            rows_loaded = self.loader.load_data(
                data=transformed_data,
                database=database,
                table_name=table,
                mode="append"
            )
            
            logger.info(f"Successfully loaded {rows_loaded} rows to {database}.{table}")
            
        except Exception as e:
            logger.exception(f"Error in FSA job: {str(e)}")
            # Could implement notification or alerting here
        finally:
            # Clean up resources
            if hasattr(self.loader, 'disconnect'):
                self.loader.disconnect()