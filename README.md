# Apache Airflow ETL Pipeline Setup Guide

This guide will help you set up your S3 to PostgreSQL ETL pipeline using Apache Airflow with Docker on Windows.

## Prerequisites

1. **Docker Desktop for Windows** - Download and install from [docker.com](https://www.docker.com/products/docker-desktop/)
2. **Windows PowerShell** (comes with Windows)
3. **AWS Account** with S3 access
4. **PostgreSQL Database** (Your Database)

## Quick Start

###  Project Structure
Create a new directory for your project and organize files as follows:
```
airflow-etl/
├── docker-compose.yml
├── .env
├── setup-airflow.ps1
├── dags/
│   └── s3_postgres_etl_dag.py
├── logs/
├── plugins/
└── config/
```

### Run Setup Script
Open PowerShell as Administrator and navigate to your project directory:

```powershell
cd C:\path\to\your\airflow-etl

Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

.\setup-airflow.ps1
```
Choose one of the services by putting their designated index number which is 1 to 7:
- 1
- 2
- 3


Chose one from the list that will show in the terminal.

### Access Airflow
Once services are running:
- **Airflow Web UI**: http://localhost:8080
- **Username**: admin
- **Password**: admin

## Detailed Configuration

### Setting up Connections in Airflow

After Airflow is running, you need to configure connections:

1. **AWS Connection**:
   - Go to Admin → Connections
   - Click "+" to add new connection
   - Connection Id: `aws_default`
   - Connection Type: `Amazon Web Services`
   - Login: Your AWS Access Key ID
   - Password: Your AWS Secret Access Key
   - Extra: `{"region_name": "us-east-1"}`

2. **PostgreSQL Connection**:
   - Connection Id: `postgres_default`
   - Connection Type: `Postgres`
   - Host: Your PostgreSQL host
   - Database: Your database name
   - Login: Your username
   - Password: Your password
   - Port: 5432

### Setting up Variables

Go to Admin → Variables and add:
- `BUCKET_NAME`: Your S3 bucket name

## DAG Configuration

The DAG is configured to:
- Run daily at 10 am
- Process files from S3 based on filename date patterns
- Merge all files for a given date
- Load data into PostgreSQL table `table_name`
- Create processing logs in `data_processing_log` table

### DAG Tasks:
1. **create_tables**: Creates necessary tables if they don't exist
2. **check_s3_files**: Verifies files exist for the processing date
3. **extract_and_transform**: Downloads, processes, and merges S3 files
4. **load_to_postgres**: Loads processed data to PostgreSQL
5. **analyze_bucket_dates**: (Optional) Analyzes available dates in bucket


## Monitoring and Troubleshooting

### Common Commands

```powershell
# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Restart specific service
docker-compose restart airflow-scheduler

# Access container
docker-compose exec airflow-webserver bash

# Stop all services
docker-compose down

# Clean everything (removes data!)
docker-compose down -v --remove-orphans
```

### Troubleshooting

1. **Memory Issues**: Ensure Docker Desktop has at least 4GB RAM allocated
2. **Permission Issues**: Run PowerShell as Administrator
3. **Connection Issues**: Verify your AWS/PostgreSQL credentials
4. **DAG Not Appearing**: Check the `dags/` directory and file syntax
5. **Task Failures**: Check logs in Airflow UI under Graph View → Task Logs


### Flowchart
```
flowchart TD
    A[Start ETL Pipeline] --> B[Create Tables]
    B --> C[Check Existing Data]
    C --> D[Check S3 Files]
    D --> E{Files Found?}
    
    E -->|No| F[Log Warning & Skip]
    E -->|Yes| G[Extract & Transform Data]
    
    G --> H[Process Files]
    H --> I[Decompress .gz Files]
    I --> J[Clean Column Names]
    J --> K[Add Metadata]
    K --> L[Convert Timestamps]
    L --> M[Remove Duplicates]
    M --> N[Save to Temp CSV]
    
    N --> O[Load to PostgreSQL]
    O --> P{Table Exists?}
    
    P -->|No| Q[Create Table]
    P -->|Yes| R{Data Exists for Date?}
    
    Q --> S[Load Data via COPY]
    R -->|Yes| T[Delete Existing Data]
    R -->|No| S
    T --> S
    
    S --> U{COPY Successful?}
    U -->|Yes| V[Verify Load]
    U -->|No| W[Fallback: INSERT Method]
    W --> V
    
    V --> X[Update Processing Log]
    X --> Y[Clean Temp Files]
    Y --> Z[End]
    
    F --> Z
    
    AA[Analyze Bucket Dates] -.->|Optional/Manual| BB[List All S3 Files]
    BB -.-> CC[Extract Date Patterns]
    CC -.-> DD[Generate Date Summary]
    
    style A fill:#e1f5fe
    style Z fill:#c8e6c9
    style F fill:#ffecb3
    style G fill:#f3e5f5
    style O fill:#f3e5f5
    style S fill:#e8f5e8
    style W fill:#fff3e0
    style AA fill:#f1f8e9
```

### Scaling for Large Files

For large datasets, consider:
- Increasing Docker memory allocation
- Adjusting `chunk_size` in the DAG
- Using Airflow's XCom backend for large data
- Implementing file compression/decompression optimizations

## Manual Execution

To run the DAG manually:
1. Go to Airflow UI
2. Find `s3_postgres_etl_pipeline` DAG
3. Toggle it ON
4. Click "Trigger DAG" button
5. Monitor progress in Graph View

## Data Validation

The pipeline includes:
- Duplicate removal
- Empty column cleanup
- Data type conversion for timestamps
- Processing logs for audit trail
