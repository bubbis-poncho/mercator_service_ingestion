# Data Ingestion Service

A modular and extensible service for ingesting data from multiple sources and storing it in various formats.

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

- **Multiple storage options**:
  - Apache Iceberg (data lake format)
  - PostgreSQL (relational database)
  - Hugegraph (graph database)
  - Easily extendable to support other formats

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
- Docker
- Docker Compose

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/data-ingestion-service.git
cd data-ingestion-service

# Run the setup script (interactive)
chmod +x setup.sh
./setup.sh
```

The setup script will guide you through configuring your data ingestion service, including selecting which storage systems to enable (Iceberg, PostgreSQL, Hugegraph).

### Configuration

The service is configured using YAML files:

1. Create job configuration files in the `config/jobs` directory
2. Create source templates in the `config/templates` directory
3. Define source configurations in the `config/sources` directory

See the `config` directory for examples.

## Storage Options

### Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets. It provides:

- Schema evolution
- Hidden partitioning
- Time travel and snapshot isolation
- Full SQL support

To use Iceberg storage, configure your job with:

```yaml
loader:
  type: "iceberg"
  database: "your_database"
  table: "your_table"
  write_mode: "append"  # or "overwrite", "merge"
  config:
    spark_config:
      spark.driver.memory: "4g"
      spark.executor.memory: "4g"
    catalog_name: "hive_prod"
    warehouse_location: "/warehouse/tablespace/external/hive"
```

### PostgreSQL

PostgreSQL is a powerful, open-source relational database system with over 30 years of active development. Use it when you need:

- Relational data structure
- ACID compliance
- Complex queries
- Strong data consistency

To use PostgreSQL storage, configure your job with:

```yaml
loader:
  type: "postgres"
  host: "${POSTGRES_HOST:-postgres}"
  port: "${POSTGRES_PORT:-5432}"
  username: "${POSTGRES_USER:-postgres}"
  password: "${POSTGRES_PASSWORD:-postgres}"
  database: "${POSTGRES_DB:-ingestion}"
  schema: "public"
  table: "your_table"
  write_mode: "append"  # or "replace", "merge"
  merge_keys:  # Required for "merge" mode
    - "id_column"
```

### Hugegraph

[Hugegraph](https://hugegraph.apache.org/docs/quickstart/) is a graph database designed for storing complex, highly connected data. Perfect for:

- Social networks
- Knowledge graphs
- Recommendation systems
- Pattern recognition

To use Hugegraph storage, configure your job with:

```yaml
loader:
  type: "hugegraph"
  host: "${HUGEGRAPH_HOST:-hugegraph}"
  port: "${HUGEGRAPH_PORT:-8080}"
  graph: "${HUGEGRAPH_GRAPH:-ingestion}"
  data_type: "vertex"  # or "edge" for relationships
  # For vertices:
  auto_create_schema: true
  # For edges:
  source_key: "from_id"  # Column with source vertex ID
  target_key: "to_id"    # Column with target vertex ID
```

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
    "job_id": "financial_data",
    "job_config": {
      "enabled": true,
      "cron_expression": "0 5 * * *",
      "source": {
        "type": "api",
        "base_url": "https://api.example.com/v1",
        "endpoint": "financial-data"
      },
      "transformer": {
        "type": "base",
        "date_columns": ["date"],
        "numeric_columns": ["amount", "fee"]
      },
      "loader": {
        "type": "postgres",
        "database": "financial_data",
        "table": "transactions"
      }
    }
  }'
```

### Example: Executing a Job

```bash
curl -X POST "http://localhost:8000/jobs/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "financial_data",
    "parameters": {
      "source": {
        "endpoint": "financial-data/daily"
      }
    }
  }'
```

## Customizing Docker Compose

You can enable or disable specific storage systems by modifying the environment variables in the `.env` file:

```
# .env file
ENABLE_ICEBERG=true
ENABLE_POSTGRES=true
ENABLE_HUGEGRAPH=false
```

Or start Docker Compose with specific profiles:

```bash
# Start with only PostgreSQL support
docker-compose --profile postgres up -d

# Start with both Iceberg and Hugegraph
docker-compose --profile iceberg --profile hugegraph up -d
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
├── docker-compose.yml         # Docker Compose configuration
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.