import logging
import os
import yaml
import glob
from typing import Dict, List, Optional, Any
import json

from .fsa_job import FSAJobScheduler
from .data_ingestion_job import DataIngestionJob

logger = logging.getLogger(__name__)

class JobManager:
    """
    Manager for all data ingestion jobs.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize job manager.
        
        Args:
            config_path: Path to job configuration file or directory
        """
        self.config_path = config_path or os.environ.get("JOB_CONFIG_PATH", "config/jobs.yaml")
        self.jobs = {}
        self.config = {"jobs": {}}
        self.load_config()
    
    def load_config(self) -> None:
        """Load job configuration from file or directory."""
        try:
            # If config_path is a directory, load all yaml files
            if os.path.isdir(self.config_path):
                self.config = {"jobs": {}}
                for file_path in glob.glob(os.path.join(self.config_path, "*.yaml")):
                    with open(file_path, 'r') as f:
                        job_config = yaml.safe_load(f)
                        if job_config and isinstance(job_config, dict):
                            # Extract job_id from filename
                            job_id = os.path.splitext(os.path.basename(file_path))[0]
                            self.config["jobs"][job_id] = job_config
                
                logger.info(f"Loaded {len(self.config['jobs'])} job configurations from {self.config_path}")
            else:
                # Load single config file
                with open(self.config_path, 'r') as f:
                    self.config = yaml.safe_load(f)
                logger.info(f"Loaded job configuration from {self.config_path}")
        except Exception as e:
            logger.error(f"Error loading job configuration: {str(e)}")
            self.config = {"jobs": {}}
    
    def init_jobs(self) -> None:
        """Initialize all jobs from configuration."""
        if "jobs" not in self.config:
            logger.warning("No jobs section in configuration")
            return
            
        jobs_config = self.config["jobs"]
        
        for job_id, job_config in jobs_config.items():
            try:
                # Skip disabled jobs
                if not job_config.get("enabled", True):
                    logger.info(f"Skipping disabled job: {job_id}")
                    continue
                
                # Check job type
                job_type = job_config.get("type", "generic")
                
                if job_type == "fsa_insider_trading":
                    # Legacy FSA job
                    logger.info(f"Initializing FSA insider trading job: {job_id}")
                    self.jobs[job_id] = FSAJobScheduler(job_config)
                else:
                    # Generic data ingestion job
                    logger.info(f"Initializing data ingestion job: {job_id}")
                    self.jobs[job_id] = DataIngestionJob(job_id, job_config)
                
            except Exception as e:
                logger.error(f"Error initializing job {job_id}: {str(e)}")
    
    def start_all(self) -> None:
        """Start all enabled jobs."""
        for job_id, job in self.jobs.items():
            try:
                job.start()
                logger.info(f"Started job: {job_id}")
            except Exception as e:
                logger.error(f"Error starting job {job_id}: {str(e)}")
    
    def stop_all(self) -> None:
        """Stop all running jobs."""
        for job_id, job in self.jobs.items():
            try:
                job.stop()
                logger.info(f"Stopped job: {job_id}")
            except Exception as e:
                logger.error(f"Error stopping job {job_id}: {str(e)}")
    
    def get_job_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status of all jobs.
        
        Returns:
            Dictionary with job status information
        """
        status = {}
        for job_id, job in self.jobs.items():
            if hasattr(job, 'scheduler'):
                job_status = {
                    "running": job.scheduler.running,
                    "next_run": None,
                    "job_type": job.__class__.__name__,
                    "enabled": job.enabled if hasattr(job, 'enabled') else True
                }
                
                # Get next run time
                for job_instance in job.scheduler.get_jobs():
                    if job_instance.id == job.job_name:
                        job_status["next_run"] = job_instance.next_run_time.isoformat() if job_instance.next_run_time else None
                        break
                
                # Get metadata if available
                metadata_file = f"metadata/job_{job_id}_metadata.yaml"
                if os.path.exists(metadata_file):
                    try:
                        with open(metadata_file, 'r') as f:
                            metadata = yaml.safe_load(f) or {}
                            
                        job_status["last_run"] = metadata.get("last_run")
                        job_status["last_successful_run"] = metadata.get("last_successful_run")
                        
                        if "runs" in metadata and metadata["runs"]:
                            last_run = metadata["runs"][-1]
                            job_status["last_status"] = last_run.get("status")
                            job_status["last_run_rows"] = last_run.get("rows_loaded", 0)
                    except Exception as e:
                        logger.error(f"Error reading job metadata: {str(e)}")
                
                status[job_id] = job_status
        
        return status
    
    def get_job_details(self, job_id: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Dictionary with job details
        """
        if job_id not in self.jobs:
            return {"error": f"Job {job_id} not found"}
            
        job = self.jobs[job_id]
        
        # Basic job info
        job_info = {
            "job_id": job_id,
            "job_type": job.__class__.__name__,
            "enabled": job.enabled if hasattr(job, 'enabled') else True,
            "running": job.scheduler.running if hasattr(job, 'scheduler') else False,
            "config": self.config["jobs"].get(job_id, {})
        }
        
        # Add run history from metadata
        metadata_file = f"metadata/job_{job_id}_metadata.yaml"
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = yaml.safe_load(f) or {}
                    
                job_info["metadata"] = metadata
            except Exception as e:
                logger.error(f"Error reading job metadata: {str(e)}")
        
        return job_info
    
    def run_job_now(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> bool:
        """
        Run a specific job immediately.
        
        Args:
            job_id: Job identifier
            parameters: Optional parameters to override job configuration
            
        Returns:
            True if job was triggered, False otherwise
        """
        if job_id in self.jobs:
            try:
                self.jobs[job_id].run_now(parameters)
                logger.info(f"Triggered immediate run of job: {job_id}")
                return True
            except Exception as e:
                logger.error(f"Error triggering job {job_id}: {str(e)}")
                return False
        else:
            logger.warning(f"Job not found: {job_id}")
            return False
    
    def create_job(self, job_id: str, job_config: Dict[str, Any]) -> bool:
        """
        Create a new job.
        
        Args:
            job_id: Job identifier
            job_config: Job configuration
            
        Returns:
            True if job was created, False otherwise
        """
        # Check if job already exists
        if job_id in self.config["jobs"]:
            logger.warning(f"Job {job_id} already exists")
            return False
            
        try:
            # Add job to configuration
            self.config["jobs"][job_id] = job_config
            
            # Save configuration
            if os.path.isdir(self.config_path):
                # Save as individual file in config directory
                file_path = os.path.join(self.config_path, f"{job_id}.yaml")
                with open(file_path, 'w') as f:
                    yaml.dump(job_config, f)
            else:
                # Update main config file
                with open(self.config_path, 'w') as f:
                    yaml.dump(self.config, f)
            
            # Initialize and start the job
            job_type = job_config.get("type", "generic")
            
            if job_type == "fsa_insider_trading":
                self.jobs[job_id] = FSAJobScheduler(job_config)
            else:
                self.jobs[job_id] = DataIngestionJob(job_id, job_config)
                
            if job_config.get("enabled", True):
                self.jobs[job_id].start()
                
            logger.info(f"Created and initialized job: {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating job {job_id}: {str(e)}")
            return False
    
    def update_job(self, job_id: str, job_config: Dict[str, Any]) -> bool:
        """
        Update an existing job.
        
        Args:
            job_id: Job identifier
            job_config: Updated job configuration
            
        Returns:
            True if job was updated, False otherwise
        """
        # Check if job exists
        if job_id not in self.config["jobs"]:
            logger.warning(f"Job {job_id} not found")
            return False
            
        try:
            # Stop the existing job
            if job_id in self.jobs:
                self.jobs[job_id].stop()
                
            # Update configuration
            self.config["jobs"][job_id] = job_config
            
            # Save configuration
            if os.path.isdir(self.config_path):
                # Save as individual file in config directory
                file_path = os.path.join(self.config_path, f"{job_id}.yaml")
                with open(file_path, 'w') as f:
                    yaml.dump(job_config, f)
            else:
                # Update main config file
                with open(self.config_path, 'w') as f:
                    yaml.dump(self.config, f)
            
            # Re-initialize the job
            job_type = job_config.get("type", "generic")
            
            if job_type == "fsa_insider_trading":
                self.jobs[job_id] = FSAJobScheduler(job_config)
            else:
                self.jobs[job_id] = DataIngestionJob(job_id, job_config)
                
            # Start if enabled
            if job_config.get("enabled", True):
                self.jobs[job_id].start()
                
            logger.info(f"Updated job: {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating job {job_id}: {str(e)}")
            return False
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if job was deleted, False otherwise
        """
        # Check if job exists
        if job_id not in self.config["jobs"]:
            logger.warning(f"Job {job_id} not found")
            return False
            
        try:
            # Stop the job if running
            if job_id in self.jobs:
                self.jobs[job_id].stop()
                del self.jobs[job_id]
                
            # Remove from configuration
            del self.config["jobs"][job_id]
            
            # Save configuration
            if os.path.isdir(self.config_path):
                # Remove individual file from config directory
                file_path = os.path.join(self.config_path, f"{job_id}.yaml")
                if os.path.exists(file_path):
                    os.remove(file_path)
            else:
                # Update main config file
                with open(self.config_path, 'w') as f:
                    yaml.dump(self.config, f)
            
            logger.info(f"Deleted job: {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting job {job_id}: {str(e)}")
            return False