import os
import sys
import logging
import argparse
import signal
import time
import uvicorn
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ingestion-service")

# Add src directory to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from jobs.job_manager import JobManager
from api.app import app

# Global reference to job manager for graceful shutdown
job_manager = None

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Data Ingestion Service")
    
    parser.add_argument(
        "--config", 
        type=str,
        default=os.environ.get("JOB_CONFIG_PATH", "config/jobs.yaml"),
        help="Path to job configuration file or directory"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("API_PORT", 8000)),
        help="Port for the API server"
    )
    
    parser.add_argument(
        "--host",
        type=str,
        default=os.environ.get("API_HOST", "0.0.0.0"),
        help="Host for the API server"
    )
    
    parser.add_argument(
        "--api-only",
        action="store_true",
        help="Run only the API server without starting scheduled jobs"
    )
    
    parser.add_argument(
        "--job-only",
        action="store_true",
        help="Run only the job scheduler without starting the API server"
    )
    
    parser.add_argument(
        "--run-job",
        type=str,
        help="Run a specific job immediately and exit"
    )
    
    return parser.parse_args()

def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        if job_manager:
            logger.info("Stopping all jobs...")
            job_manager.stop_all()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def run_api_server(host: str, port: int):
    """Run the API server."""
    logger.info(f"Starting API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)

def run_job_scheduler(config_path: str):
    """Run the job scheduler."""
    global job_manager
    
    logger.info(f"Initializing job manager with config: {config_path}")
    job_manager = JobManager(config_path)
    
    logger.info("Initializing jobs...")
    job_manager.init_jobs()
    
    logger.info("Starting all enabled jobs...")
    job_manager.start_all()
    
    # Keep the process running
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Stopping all jobs...")
        job_manager.stop_all()

def run_specific_job(config_path: str, job_id: str):
    """Run a specific job immediately and exit."""
    global job_manager
    
    logger.info(f"Initializing job manager with config: {config_path}")
    job_manager = JobManager(config_path)
    
    logger.info("Initializing jobs...")
    job_manager.init_jobs()
    
    if job_id not in job_manager.jobs:
        logger.error(f"Job {job_id} not found")
        return
    
    logger.info(f"Running job {job_id}...")
    job_manager.run_job_now(job_id)
    
    # Wait for job to complete (simple approach)
    # In production, you'd want to monitor the job status
    logger.info("Waiting for job to complete...")
    time.sleep(10)
    
    logger.info("Stopping all jobs...")
    job_manager.stop_all()

def main():
    """Main entry point."""
    # Parse arguments
    args = parse_arguments()
    
    # Setup signal handlers
    setup_signal_handlers()
    
    # Handle single job run
    if args.run_job:
        run_specific_job(args.config, args.run_job)
        return
    
    # Handle job-only mode
    if args.job_only:
        run_job_scheduler(args.config)
        return
    
    # Handle API-only mode
    if args.api_only:
        run_api_server(args.host, args.port)
        return
    
    # Handle normal mode (both API and jobs)
    # Start job scheduler in a separate thread
    import threading
    
    job_thread = threading.Thread(
        target=run_job_scheduler,
        args=(args.config,),
        daemon=True
    )
    job_thread.start()
    
    # Run API server in main thread
    run_api_server(args.host, args.port)

if __name__ == "__main__":
    main()