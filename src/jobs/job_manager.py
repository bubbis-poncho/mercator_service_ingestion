
import logging
import os
import yaml
from typing import Dict, Any, List, Optional
import json

from .fsa_job import FSAJobScheduler

logger = logging.getLogger(__name__)

class JobManager:
    """
    Manager for all data ingestion jobs.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize job manager.
        
        Args:
            config_path: Path to job configuration file
        """
        self.config_path = config_path or os.environ.get("JOB_CONFIG_PATH", "config/jobs.yaml")
        self.jobs = {}
        self.load_config()
    
    def load_config(self) -> None:
        """Load job configuration from file."""
        try:
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
        
        # Initialize FSA job if configured
        if "fsa_insider_trading" in jobs_config and jobs_config["fsa_insider_trading"]["enabled"]:
            logger.info("Initializing FSA insider trading job")
            self.jobs["fsa_insider_trading"] = FSAJobScheduler(jobs_config["fsa_insider_trading"])
    
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
                    "next_run": None
                }
                
                # Get next run time
                for job_instance in job.scheduler.get_jobs():
                    if job_instance.id == job.job_name:
                        job_status["next_run"] = job_instance.next_run_time.isoformat() if job_instance.next_run_time else None
                        break
                
                status[job_id] = job_status
        
        return status
    
    def run_job_now(self, job_id: str) -> bool:
        """
        Run a specific job immediately.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if job was triggered, False otherwise
        """
        if job_id in self.jobs:
            try:
                self.jobs[job_id].run_now()
                logger.info(f"Triggered immediate run of job: {job_id}")
                return True
            except Exception as e:
                logger.error(f"Error triggering job {job_id}: {str(e)}")
                return False
        else:
            logger.warning(f"Job not found: {job_id}")
            return False