# PowerShell script to set up and run Apache Airflow with Docker
# Run this script from the directory containing your docker-compose.yml file

Write-Host "=== Apache Airflow Docker Setup ===" -ForegroundColor Green

# Check if Docker is running
Write-Host "Checking Docker..." -ForegroundColor Yellow
try {
    docker --version | Out-Host
    docker-compose --version | Out-Host
} catch {
    Write-Host "ERROR: Docker or Docker Compose not found. Please install Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Create necessary directories
Write-Host "Creating directory structure..." -ForegroundColor Yellow
$directories = @("dags", "logs", "plugins", "config")
foreach ($dir in $directories) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "Created directory: $dir" -ForegroundColor Cyan
    } else {
        Write-Host "Directory already exists: $dir" -ForegroundColor Cyan
    }
}

# Check if .env file exists
if (!(Test-Path ".env")) {
    Write-Host "WARNING: .env file not found. Please create one with your configuration." -ForegroundColor Red
    Write-Host "Example .env file should contain:" -ForegroundColor Yellow
    Write-Host "AWS_ACCESS_KEY_ID=your_key" -ForegroundColor Gray
    Write-Host "AWS_SECRET_ACCESS_KEY=your_secret" -ForegroundColor Gray
    Write-Host "BUCKET_ADJUST_NAME=your_bucket" -ForegroundColor Gray
    Write-Host "DW_PG_HOST=your_postgres_host" -ForegroundColor Gray
    Write-Host "... and other configuration variables" -ForegroundColor Gray
    Write-Host ""
    Read-Host "Press Enter to continue anyway (you can add .env later)"
}

# Set AIRFLOW_UID for Windows
$env:AIRFLOW_UID = "50000"
Write-Host "Set AIRFLOW_UID to 50000" -ForegroundColor Cyan

# Initialize Airflow
Write-Host "Initializing Airflow..." -ForegroundColor Yellow
Write-Host "This may take several minutes on first run..." -ForegroundColor Gray
docker-compose up airflow-init

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Airflow initialization failed!" -ForegroundColor Red
    exit 1
}

Write-Host "Airflow initialization completed successfully!" -ForegroundColor Green

# Function to start services
function Start-AirflowServices {
    Write-Host "Starting Airflow services..." -ForegroundColor Yellow
    docker-compose up -d
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "All services started successfully!" -ForegroundColor Green
        Write-Host ""
        Write-Host "=== Access Information ===" -ForegroundColor Green
        Write-Host "Airflow Web UI: http://localhost:8080" -ForegroundColor Cyan
        Write-Host "Username: airflow" -ForegroundColor Cyan
        Write-Host "Password: airflow" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "PostgreSQL (Airflow DB): localhost:5432" -ForegroundColor Cyan
        Write-Host "Database: airflow" -ForegroundColor Cyan
        Write-Host "Username: airflow" -ForegroundColor Cyan
        Write-Host "Password: airflow" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "To stop services: docker-compose down" -ForegroundColor Yellow
        Write-Host "To view logs: docker-compose logs -f [service_name]" -ForegroundColor Yellow
        Write-Host "To restart services: docker-compose restart" -ForegroundColor Yellow
    } else {
        Write-Host "ERROR: Failed to start services!" -ForegroundColor Red
        exit 1
    }
}

# Function to stop services
function Stop-AirflowServices {
    Write-Host "Stopping Airflow services..." -ForegroundColor Yellow
    docker-compose down
    Write-Host "Services stopped successfully!" -ForegroundColor Green
}

# Function to show service status
function Show-ServiceStatus {
    Write-Host "=== Service Status ===" -ForegroundColor Green
    docker-compose ps
}

# Function to show logs
function Show-Logs {
    param([string]$ServiceName = "")
    
    if ($ServiceName) {
        Write-Host "Showing logs for $ServiceName..." -ForegroundColor Yellow
        docker-compose logs -f $ServiceName
    } else {
        Write-Host "Showing logs for all services..." -ForegroundColor Yellow
        docker-compose logs -f
    }
}

# Main menu
Write-Host ""
Write-Host "=== What would you like to do? ===" -ForegroundColor Green
Write-Host "1. Start Airflow services" -ForegroundColor Cyan
Write-Host "2. Stop Airflow services" -ForegroundColor Cyan
Write-Host "3. Check service status" -ForegroundColor Cyan
Write-Host "4. View logs" -ForegroundColor Cyan
Write-Host "5. Restart services" -ForegroundColor Cyan
Write-Host "6. Clean up (remove all containers and volumes)" -ForegroundColor Cyan
Write-Host "7. Exit" -ForegroundColor Cyan

$choice = Read-Host "Enter your choice (1-7)"

switch ($choice) {
    "1" { Start-AirflowServices }
    "2" { Stop-AirflowServices }
    "3" { Show-ServiceStatus }
    "4" { 
        Write-Host "Available services: airflow-webserver, airflow-scheduler, airflow-worker, postgres, redis" -ForegroundColor Yellow
        $service = Read-Host "Enter service name (or press Enter for all)"
        Show-Logs -ServiceName $service
    }
    "5" { 
        Write-Host "Restarting services..." -ForegroundColor Yellow
        docker-compose restart
        Write-Host "Services restarted!" -ForegroundColor Green
    }
    "6" { 
        Write-Host "WARNING: This will remove all containers, volumes, and data!" -ForegroundColor Red
        $confirm = Read-Host "Are you sure? (y/N)"
        if ($confirm -eq "y" -or $confirm -eq "Y") {
            docker-compose down -v --remove-orphans
            docker system prune -f
            Write-Host "Cleanup completed!" -ForegroundColor Green
        } else {
            Write-Host "Cleanup cancelled." -ForegroundColor Yellow
        }
    }
    "7" { 
        Write-Host "Goodbye!" -ForegroundColor Green
        exit 0
    }
    default { 
        Write-Host "Invalid choice! Starting services by default..." -ForegroundColor Yellow
        Start-AirflowServices
    }
}

Write-Host ""
Write-Host "=== Additional Commands ===" -ForegroundColor Green
Write-Host "To run this script again: .\setup-airflow.ps1" -ForegroundColor Cyan
Write-Host "To enter a container: docker-compose exec airflow-webserver bash" -ForegroundColor Cyan
Write-Host "To see all containers: docker ps" -ForegroundColor Cyan