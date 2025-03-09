from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query, status, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Optional, Any
import uvicorn
import logging
import os
import yaml
from pydantic import BaseModel, Field
import json
from datetime import datetime

# Import job management
from ..jobs.job_manager import JobManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ingestion-api")

# Initialize FastAPI application
app = FastAPI(
    title="Data Ingestion Service API",
    description="API for managing data ingestion jobs and sources",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for API requests/responses
class JobStatus(BaseModel):
    running: bool
    next_run: Optional[str] = None

class JobStatusResponse(BaseModel):
    jobs: Dict[str, JobStatus]

class SourceConfig(BaseModel):
    source_id: str
    name: str
    description: Optional[str] = None
    enabled: bool = True
    cron_expression: Optional[str] = None
    scraper: Dict[str, Any]
    transformer: Dict[str, Any]
    loader: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

class SourceListResponse(BaseModel):
    sources: List[Dict[str, Any]]

class ExecuteJobRequest(BaseModel):
    job_id: str
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict)

class ExecuteJobResponse(BaseModel):
    job_id: str
    status: str
    execution_id: str = None
    scheduled_time: Optional[str] = None
    message: Optional[str] = None

class JobConfig(BaseModel):
    type: Optional[str] = "generic"
    enabled: bool = True
    cron_expression: Optional[str] = None
    source: Dict[str, Any]
    transformer: Optional[Dict[str, Any]] = None
    loader: Dict[str, Any]
    monitoring: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

class JobDetails(BaseModel):
    job_id: str
    job_type: str
    enabled: bool
    running: bool
    config: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

# Dependency to get job manager instance
def get_job_manager():
    config_path = os.environ.get("JOB_CONFIG_PATH", "config/jobs.yaml")
    job_manager = JobManager(config_path)
    job_manager.load_config()
    job_manager.init_jobs()
    return job_manager

# API routes

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "service": "ingestion-service", "timestamp": datetime.now().isoformat()}

@app.get("/jobs/status", response_model=JobStatusResponse)
async def get_jobs_status(job_manager: JobManager = Depends(get_job_manager)):
    """Get the status of all configured jobs"""
    status = job_manager.get_job_status()
    return {"jobs": status}

@app.get("/jobs", response_model=Dict[str, Any])
async def list_jobs(job_manager: JobManager = Depends(get_job_manager)):
    """Get a list of all configured jobs with basic info"""
    status = job_manager.get_job_status()
    return {"jobs": status}

@app.get("/jobs/{job_id}", response_model=JobDetails)
async def get_job(job_id: str, job_manager: JobManager = Depends(get_job_manager)):
    """Get detailed information for a specific job"""
    # Check if job exists
    if job_id not in job_manager.jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    return job_manager.get_job_details(job_id)

@app.post("/jobs/execute", response_model=ExecuteJobResponse)
async def execute_job(
    request: ExecuteJobRequest,
    background_tasks: BackgroundTasks,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Execute a job immediately in the background"""
    job_id = request.job_id
    
    # Check if job exists
    if job_id not in job_manager.jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    # Schedule job execution in background
    execution_id = f"{job_id}_manual_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Add job parameters if specified
    background_tasks.add_task(job_manager.run_job_now, job_id, request.parameters)
    
    return {
        "job_id": job_id,
        "status": "scheduled",
        "execution_id": execution_id,
        "scheduled_time": datetime.now().isoformat(),
        "message": f"Job {job_id} scheduled for immediate execution"
    }

@app.post("/jobs", status_code=status.HTTP_201_CREATED)
async def create_job(
    job_id: str,
    job_config: JobConfig,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Create a new job configuration"""
    # Check if job already exists
    if job_id in job_manager.jobs:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Job {job_id} already exists"
        )
    
    # Create job
    success = job_manager.create_job(job_id, job_config.dict(exclude_unset=True))
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create job {job_id}"
        )
    
    return {"message": f"Job {job_id} created successfully"}

@app.put("/jobs/{job_id}")
async def update_job(
    job_id: str,
    job_config: JobConfig,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Update an existing job configuration"""
    # Check if job exists
    if job_id not in job_manager.jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    # Update job
    success = job_manager.update_job(job_id, job_config.dict(exclude_unset=True))
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update job {job_id}"
        )
    
    return {"message": f"Job {job_id} updated successfully"}

@app.delete("/jobs/{job_id}")
async def delete_job(
    job_id: str,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Delete a job configuration"""
    # Check if job exists
    if job_id not in job_manager.jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    # Delete job
    success = job_manager.delete_job(job_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete job {job_id}"
        )
    
    return {"message": f"Job {job_id} deleted successfully"}

@app.get("/sources", response_model=SourceListResponse)
async def list_sources():
    """List all configured data sources"""
    sources = []
    sources_dir = "config/sources"
    
    # Walk through source config files
    if os.path.exists(sources_dir):
        for root, _, files in os.walk(sources_dir):
            for file in files:
                if file.endswith(('.yaml', '.yml')):
                    try:
                        with open(os.path.join(root, file), 'r') as f:
                            source_config = yaml.safe_load(f)
                            if source_config and 'source_id' in source_config:
                                # Add source file path
                                source_config['config_file'] = os.path.join(root, file)
                                # Add relative path category
                                rel_path = os.path.relpath(root, sources_dir)
                                if rel_path != '.':
                                    source_config['category'] = rel_path.replace(os.sep, '/')
                                sources.append(source_config)
                    except Exception as e:
                        logger.error(f"Error reading source config {file}: {str(e)}")
    
    return {"sources": sources}

@app.get("/sources/{source_id}")
async def get_source(source_id: str):
    """Get configuration for a specific data source"""
    sources_dir = "config/sources"
    
    # Search for source config
    for root, _, files in os.walk(sources_dir):
        for file in files:
            if file.endswith(('.yaml', '.yml')):
                try:
                    with open(os.path.join(root, file), 'r') as f:
                        source_config = yaml.safe_load(f)
                        if source_config and source_config.get('source_id') == source_id:
                            return source_config
                except Exception as e:
                    logger.error(f"Error reading source config {file}: {str(e)}")
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Source {source_id} not found"
    )

@app.post("/sources")
async def create_source(source: SourceConfig, response: Response):
    """Create a new data source configuration"""
    sources_dir = "config/sources"
    
    # Ensure directory exists
    if not os.path.exists(sources_dir):
        os.makedirs(sources_dir)
    
    # Check if source already exists
    for root, _, files in os.walk(sources_dir):
        for file in files:
            if file.endswith(('.yaml', '.yml')):
                try:
                    with open(os.path.join(root, file), 'r') as f:
                        existing_config = yaml.safe_load(f)
                        if existing_config and existing_config.get('source_id') == source.source_id:
                            response.status_code = status.HTTP_409_CONFLICT
                            return {"error": f"Source with ID {source.source_id} already exists"}
                except Exception:
                    pass
    
    # Create category directory if specified
    category = None
    if "metadata" in source.dict() and source.metadata and "category" in source.metadata:
        category = source.metadata["category"]
        category_dir = os.path.join(sources_dir, category)
        if not os.path.exists(category_dir):
            os.makedirs(category_dir)
    
    # Determine path for new source
    file_path = os.path.join(sources_dir, f"{source.source_id}.yaml")
    if category:
        file_path = os.path.join(sources_dir, category, f"{source.source_id}.yaml")
    
    # Write configuration
    try:
        with open(file_path, 'w') as f:
            yaml.dump(source.dict(exclude_none=True), f)
        
        response.status_code = status.HTTP_201_CREATED
        return {"message": f"Source {source.source_id} created successfully", "path": file_path}
    except Exception as e:
        logger.error(f"Error creating source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create source: {str(e)}"
        )

@app.put("/sources/{source_id}")
async def update_source(source_id: str, source: SourceConfig):
    """Update an existing data source configuration"""
    if source_id != source.source_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Source ID in path must match source ID in body"
        )
    
    sources_dir = "config/sources"
    source_path = None
    
    # Find existing source
    for root, _, files in os.walk(sources_dir):
        for file in files:
            if file.endswith(('.yaml', '.yml')):
                try:
                    with open(os.path.join(root, file), 'r') as f:
                        existing_config = yaml.safe_load(f)
                        if existing_config and existing_config.get('source_id') == source_id:
                            source_path = os.path.join(root, file)
                            break
                except Exception:
                    pass
        if source_path:
            break
    
    if not source_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source {source_id} not found"
        )
    
    # Update configuration
    try:
        with open(source_path, 'w') as f:
            yaml.dump(source.dict(exclude_none=True), f)
        return {"message": f"Source {source_id} updated successfully"}
    except Exception as e:
        logger.error(f"Error updating source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update source: {str(e)}"
        )

@app.delete("/sources/{source_id}")
async def delete_source(source_id: str):
    """Delete a data source configuration"""
    sources_dir = "config/sources"
    source_path = None
    
    # Find existing source
    for root, _, files in os.walk(sources_dir):
        for file in files:
            if file.endswith(('.yaml', '.yml')):
                try:
                    with open(os.path.join(root, file), 'r') as f:
                        existing_config = yaml.safe_load(f)
                        if existing_config and existing_config.get('source_id') == source_id:
                            source_path = os.path.join(root, file)
                            break
                except Exception:
                    pass
        if source_path:
            break
    
    if not source_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source {source_id} not found"
        )
    
    # Delete configuration file
    try:
        os.remove(source_path)
        return {"message": f"Source {source_id} deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete source: {str(e)}"
        )

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)