# Data Ingestion Service

A modular and extensible service for ingesting data from multiple sources and storing it in Apache Iceberg format.

## Features

- **Multi-source data ingestion**:
  - REST APIs
  - Databases (PostgreSQL, MySQL, Oracle, SQLite, MSSQL)
  - Web scraping
  - Files (CSV, JSON, Excel, XML, Parquet, Avro, ORC)

- **Flexible transformations**:
  - Type conversion
  - Data validation
  - Column mapping
  - Custom transformations

- **Powerful storage**:
  - Apache Iceberg (default)
  - Extensible to other storage formats

- **Advanced scheduling**:
  - Cron-based scheduling
  - Manual triggering
  - Incremental loading

- **Monitoring**:
  - Job status tracking
  - Alerting for failures
  - Metrics collection

## Architecture

The service is built around these main components:

1. **Connectors**: Responsible for extracting data from various sources
2. **Transformers**: Process and transform the extracted data
3. **Loaders**: Store the transformed data in the target destination
4. **Job Manager**: Orchestrate the execution of data ingestion jobs
5. **API Interface**: Expose REST endpoints for managing jobs and configurations

## Getting Started

### Prerequisites

- Python 3.8+
- Java 11+ (for Apache Spark/Iceberg)
- Docker (optional)

### Installation

#### Using Docker (recommended)

```bash
# Build the Docker image
docker build -t data-ingestion-service .

# Run the service
docker run -d \
  -p 8000:8000 \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/metadata:/app/metadata \
  -v /path/to/warehouse:/warehouse/tablespace/external/hive \
  --name data-ingestion-service \
  data-ingestion-service
```

#### Manual Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/data-ingestion-service.git
cd data-ingestion-service

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the service
python src/main.py
```

### Configuration

The service is configured using YAML files:

1. Create job configuration files in the `config/jobs` directory
2. Create source templates in the `config/templates` directory
3. Define source configurations in the `config/sources` directory

See the `config` directory for examples.

## Usage

### API Endpoints

The service exposes the following REST API endpoints:

- `GET /health`: Check service health
- `GET /jobs`: List all jobs
- `GET /jobs/{job_id}`: Get job details
- `POST /jobs`: Create a new job
- `PUT /jobs/{job_id}`: Update an existing job
- `DELETE /jobs/{job_id}`: Delete a job
- `POST /jobs/execute`: Execute a job immediately
- `GET /sources`: List all data sources
- `GET /sources/{source_id}`: Get source details
- `POST /sources`: Create a new source
- `PUT /sources/{source_id}`: Update an existing source
- `DELETE /sources/{source_id}`: Delete a source

### Example: Creating a Job via API

```bash
curl -X POST "http://localhost:8000/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "daily_sales",
    "job_config": {
      "enabled": true,
      "cron_expression": "0 5 * * *",
      "source": {
        "type": "csv",
        "source_path": "/data/incoming/reports/",
        "file_pattern": "daily_sales_*.csv"
      },
      "transformer": {
        "type": "base",
        "date_columns": ["date"],
        "numeric_columns": ["quantity", "revenue"]
      },
      "loader": {
        "type": "iceberg",
        "database": "sales_data",
        "table": "daily_sales"
      }
    }
  }'
```

### Example: Executing a Job

```bash
curl -X POST "http://localhost:8000/jobs/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "daily_sales",
    "parameters": {
      "source": {
        "file_pattern": "daily_sales_2023-03-*.csv"
      }
    }
  }'
```

## Development

### Adding a New Connector

1. Create a new connector class in `src/connectors/`
2. Implement the required interface for your connector type
3. Register it in the `ConnectorFactory`

### Adding a New Transformer

1. Create a new transformer class in `src/transformers/source_transformers/`
2. Extend the `BaseTransformer` class
3. Register it in the `TransformerFactory`

### Adding a New Loader

1. Create a new loader class in `src/loaders/`
2. Extend the `BaseLoader` class
3. Register it in the `LoaderFactory`

## Project Structure

```
data-ingestion-service/
├── config/                    # Configuration files
│   ├── jobs/                  # Job configurations
│   ├── sources/               # Source definitions
│   └── templates/             # Configuration templates
├── src/                       # Source code
│   ├── api/                   # API interface
│   ├── connectors/            # Data source connectors
│   │   ├── api_connector/     # API connectors
│   │   ├── db_connector/      # Database connectors
│   │   ├── file_connector/    # File connectors
│   │   └── web_scraper/       # Web scraping connectors
│   ├── jobs/                  # Job definitions and scheduler
│   ├── loaders/               # Data loaders
│   ├── transformers/          # Data transformers
│   └── main.py                # Application entry point
├── metadata/                  # Job metadata and state tracking
├── Dockerfile                 # Docker configuration
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Configuration Reference

### Job Configuration

```yaml
# Basic job settings
enabled: true                   # Whether the job is enabled
cron_expression: "0 3 * * *"    # When to run the job (cron format)
type: "generic"                 # Job type (generic, fsa_insider_trading)

# Source configuration - where the data comes from
source:
  type: "api"                   # Type of source (api, db, file, web_scraper)
  # Source-specific configuration...

# Transformer configuration - how to process the data
transformer:
  type: "base"                  # Type of transformer
  date_columns: []              # Columns to convert to dates
  numeric_columns: []           # Columns to convert to numbers
  # Transformer-specific configuration...

# Loader configuration - where to store the data
loader:
  type: "iceberg"               # Type of loader (iceberg, postgres, delta)
  database: "my_database"       # Target database
  table: "my_table"             # Target table
  write_mode: "append"          # Write mode (append, overwrite, merge)
  # Loader-specific configuration...

# Monitoring configuration - alerts and notifications
monitoring:
  alert_on_failure: true        # Send alert on job failure
  alert_on_zero_records: true   # Send alert when no records are processed
  notification_channels:        # Where to send notifications
    - slack
    - email
  # Monitoring-specific configuration...

# Metadata for documentation and organization
metadata:
  owner: "Data Team"            # Team or person responsible
  contact_email: "data@example.com"  # Contact information
  tags:                         # Organization tags
    - "financial"
    - "daily"
```

### Source Types

#### API Source

```yaml
source:
  type: "api"
  base_url: "https://api.example.com/v1"
  endpoint: "data"
  auth_type: "bearer"           # none, basic, bearer, api_key
  auth_config:
    token: "${API_TOKEN}"
  pagination_strategy: "page"   # none, page, offset, cursor
  # More API-specific configuration...
```

#### Database Source

```yaml
source:
  type: "postgresql"            # postgresql, mysql, sqlite, oracle, mssql
  host: "db.example.com"
  port: 5432
  username: "${DB_USER}"
  password: "${DB_PASSWORD}"
  database: "source_db"
  schema: "public"
  table: "customers"            # Optional - can use custom query instead
  # Database-specific configuration...
```

#### File Source

```yaml
source:
  type: "csv"                   # csv, json, excel, xml, parquet, avro, orc
  source_path: "/data/files/"
  file_pattern: "*.csv"
  incremental: true             # Only process new/changed files
  # File-specific configuration...
```

#### Web Scraper Source

```yaml
source:
  type: "web_scraper"
  base_url: "https://example.com/data"
  user_agent: "Mozilla/5.0..."
  requests_per_minute: 6
  # Web scraper-specific configuration...
```

## Environment Variables

The service supports the following environment variables:

- `JOB_CONFIG_PATH`: Path to job configuration file or directory
- `API_PORT`: Port for the API server (default: 8000)
- `API_HOST`: Host for the API server (default: 0.0.0.0)
- `ICEBERG_WAREHOUSE_LOCATION`: Location for Iceberg data files

