# Use a Python base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install Java for Apache Spark/Iceberg
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Create directories
RUN mkdir -p /app/config/jobs /app/metadata /warehouse/tablespace/external/hive

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/src/
COPY config/ /app/config/

# Create volume mount points
VOLUME ["/app/config", "/app/metadata", "/warehouse/tablespace/external/hive"]

# Expose API port
EXPOSE 8000

# Environment variables
ENV PYTHONPATH=/app
ENV JOB_CONFIG_PATH=/app/config/jobs
ENV ICEBERG_WAREHOUSE_LOCATION=/warehouse/tablespace/external/hive

# Set entrypoint
ENTRYPOINT ["python", "src/main.py"]

# Default command (can be overridden)
CMD ["--host", "0.0.0.0", "--port", "8000"]