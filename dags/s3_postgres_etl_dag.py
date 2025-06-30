from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from sqlalchemy import create_engine, text
import pandas as pd
import gzip
import tempfile
import os
import re
import logging
from io import StringIO

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime("year", "month", "day"),  # Replace with actual start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True  # This will backfill from starting date to now
}

# Create the DAG
dag = DAG(
    's3_postgres_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to process S3 files and load to PostgreSQL',
    schedule_interval='0 10 * * *',  # Run daily at 10:00 AM UTC
    #schedule_interval=@daily, # Uncomment for daily runs
    max_active_runs=1,
    catchup=True,  # Enable catchup to process historical dates
    tags=['etl', 's3', 'postgres', 'data-processing']
)

def extract_date_from_filename(filename):
    """Extract date from filename pattern: prefix_YYYY-MM-DDTHHMM_suffix.csv.gz"""
    # Look for pattern like 2025-02-03T030000
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})T\d{6}', filename)
    if date_match:
        return date_match.group(1)
    
    # Fallback to simpler YYYY-MM-DD pattern
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if date_match:
        return date_match.group(1)
    
    return None

def check_s3_files(**context):
    """Check if files exist in S3 for the given date"""
    execution_date = context['ds']  # YYYY-MM-DD format
    
    # Get S3 connection
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = Variable.get('S3_BUCKET_NAME')
    
    # List objects in bucket
    objects = s3_hook.list_keys(bucket_name=bucket_name, prefix='')
    
    # Filter files by date
    matching_files = []
    for obj_key in objects:
        extracted_date = extract_date_from_filename(obj_key)
        if extracted_date == execution_date:
            matching_files.append(obj_key)
    
    logging.info(f"Found {len(matching_files)} files for date {execution_date}")
    
    if not matching_files:
        logging.warning(f"No files found for date {execution_date}")
        return False
    
    # Store matching files in XCom for next task
    context['task_instance'].xcom_push(key='matching_files', value=matching_files)
    return True

def check_existing_data(**context):
    """Check if data for this date already exists in the table"""
    execution_date = context['ds']
    
    # Get PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    table_name = Variable.get('TARGET_TABLE_NAME')
    
    # Check if table exists
    check_table_sql = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = %s
    );
    """
    
    table_exists = postgres_hook.get_first(check_table_sql, parameters=[table_name])[0]
    
    if not table_exists:
        logging.info(f"Table {table_name} does not exist, will create it")
        context['task_instance'].xcom_push(key='data_exists', value=False)
        return False
    
    # Check if data for this date already exists
    check_data_sql = f"""
    SELECT COUNT(*) FROM {table_name} 
    WHERE source_date = %s;
    """
    
    existing_count = postgres_hook.get_first(check_data_sql, parameters=[execution_date])[0]
    
    if existing_count > 0:
        logging.info(f"Found {existing_count} existing records for date {execution_date}")
        context['task_instance'].xcom_push(key='data_exists', value=True)
        context['task_instance'].xcom_push(key='existing_count', value=existing_count)
        return True
    else:
        logging.info(f"No existing data found for date {execution_date}")
        context['task_instance'].xcom_push(key='data_exists', value=False)
        return False

def extract_and_transform_data(**context):
    """Extract data from S3 files and transform it"""
    execution_date = context['ds']
    
    # Get matching files from previous task
    matching_files = context['task_instance'].xcom_pull(task_ids='check_s3_files', key='matching_files')
    
    if not matching_files:
        raise ValueError(f"No matching files found for date {execution_date}")
    
    # Initialize S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = Variable.get('S3_BUCKET_NAME')
    
    merged_dataframes = []
    total_rows = 0
    files_processed = 0
    
    for file_key in matching_files:
        try:
            logging.info(f"Processing file: {os.path.basename(file_key)}")
            
            try:
                # Get S3 object to read binary data
                s3_client = s3_hook.get_conn()
                response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                file_content = response['Body'].read()
                
                # Read file based on extension
                if file_key.endswith('.csv.gz'):
                    # Decompress gzipped content
                    decompressed_content = gzip.decompress(file_content).decode('utf-8')
                    df = pd.read_csv(StringIO(decompressed_content), low_memory=False)
                elif file_key.endswith('.csv'):
                    # Read CSV content directly
                    csv_content = file_content.decode('utf-8')
                    df = pd.read_csv(StringIO(csv_content), low_memory=False)
                else:
                    logging.warning(f"Unsupported file format: {os.path.basename(file_key)}")
                    continue
                
                # Free up memory immediately after reading
                del file_content
                if 'decompressed_content' in locals():
                    del decompressed_content
                if 'csv_content' in locals():
                    del csv_content
                
                # Clean column names
                df.columns = df.columns.str.replace(r"[\{\}]", "", regex=True).str.strip()
                
                # Add metadata
                df['source_file'] = os.path.basename(file_key)
                df['processed_date'] = datetime.now()
                df['source_date'] = execution_date
                
                merged_dataframes.append(df)
                total_rows += len(df)
                files_processed += 1
                
                logging.info(f"Processed {len(df)} rows from file")
                
            except Exception as file_error:
                logging.error(f"Error reading file content: {str(file_error)}")
                continue
                    
        except Exception as e:
            logging.error(f"Error processing file: {str(e)}")
            continue
    
    if not merged_dataframes:
        raise ValueError("No data successfully processed from any files")
    
    # Merge all dataframes
    merged_df = pd.concat(merged_dataframes, ignore_index=True, sort=False)
    logging.info(f"Merged {len(merged_df)} total rows from {files_processed} files")
    
    # Convert timestamp columns (configurable via Airflow Variable)
    timestamp_columns_str = Variable.get('TIMESTAMP_COLUMNS', '')
    timestamp_columns = [col.strip() for col in timestamp_columns_str.split(',') if col.strip()]
    
    for col in timestamp_columns:
        if col in merged_df.columns:
            if merged_df[col].dtype in ['int64', 'float64']:
                try:
                    merged_df[f'{col}_datetime'] = pd.to_datetime(merged_df[col], unit='s', errors='coerce')
                    logging.info(f"Converted {col} to {col}_datetime")
                except Exception as e:
                    logging.warning(f"Could not convert {col} to datetime: {str(e)}")
    
    # Add final metadata
    merged_df['files_merged_count'] = len(merged_dataframes)
    
    # Remove empty columns
    empty_cols = merged_df.columns[merged_df.isnull().all()].tolist()
    if empty_cols:
        logging.info(f"Removing {len(empty_cols)} empty columns")
        merged_df = merged_df.drop(columns=empty_cols)
    
    # Remove duplicates
    duplicates = merged_df.duplicated().sum()
    if duplicates > 0:
        logging.info(f"Removing {duplicates} duplicate rows")
        merged_df = merged_df.drop_duplicates()
    
    # Ensure the directory exists and add more robust file handling
    temp_dir = "/tmp"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    temp_csv_path = f"{temp_dir}/processed_data_{execution_date}.csv"
    
    # Save the data
    try:
        merged_df.to_csv(temp_csv_path, index=False)
        logging.info(f"Saved {len(merged_df)} rows to temporary file")
        
        # Verify the file was created and has content
        if not os.path.exists(temp_csv_path):
            raise FileNotFoundError(f"Failed to create temporary file")
        
        file_size = os.path.getsize(temp_csv_path)
        if file_size == 0:
            raise ValueError(f"Created file is empty")
        
        logging.info(f"File verified: {file_size} bytes")
        
    except Exception as e:
        logging.error(f"Error saving CSV file: {str(e)}")
        raise
    
    # Store metadata in XCom
    context['task_instance'].xcom_push(key='temp_csv_path', value=temp_csv_path)
    context['task_instance'].xcom_push(key='total_rows', value=len(merged_df))
    context['task_instance'].xcom_push(key='files_processed', value=files_processed)
    
    logging.info(f"Data processing complete")
    return temp_csv_path

def load_data_to_postgres(**context):
    """Load processed data to PostgreSQL with incremental loading (no table drop)"""
    execution_date = context['ds']
    
    # Get metadata from previous tasks
    temp_csv_path = context['task_instance'].xcom_pull(task_ids='extract_and_transform', key='temp_csv_path')
    total_rows = context['task_instance'].xcom_pull(task_ids='extract_and_transform', key='total_rows')
    files_processed = context['task_instance'].xcom_pull(task_ids='extract_and_transform', key='files_processed')
    data_exists = context['task_instance'].xcom_pull(task_ids='check_existing_data', key='data_exists')
    
    if not temp_csv_path or not os.path.exists(temp_csv_path):
        raise ValueError("No processed data file found")
    
    # Get PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    table_name = Variable.get('TARGET_TABLE_NAME')
    
    try:
        # Read CSV to get column names and sample data
        df_sample = pd.read_csv(temp_csv_path, nrows=100)
        columns = df_sample.columns.tolist()
        
        logging.info(f"Processing {len(columns)} columns")
        
        # Check if table exists, if not create it
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
        """
        
        table_exists = postgres_hook.get_first(check_table_sql, parameters=[table_name])[0]
        
        if not table_exists:
            logging.info(f"Creating table {table_name} as it doesn't exist")
            # Create table with all TEXT columns (safest approach for initial creation)
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join([f'"{col}" TEXT' for col in columns])}
            );
            """
            postgres_hook.run(create_table_sql)
            logging.info(f"Created table {table_name} with TEXT columns")
        else:
            logging.info(f"Table {table_name} already exists")
            
            # If data already exists for this date, delete it first (upsert behavior)
            if data_exists:
                delete_sql = f"DELETE FROM {table_name} WHERE source_date = %s"
                postgres_hook.run(delete_sql, parameters=[execution_date])
                logging.info(f"Deleted existing data for date {execution_date}")
        
        # Use COPY to load data (most efficient method)
        copy_sql = f"""
        COPY {table_name} ({', '.join([f'"{col}"' for col in columns])})
        FROM STDIN WITH CSV HEADER DELIMITER ','
        """
        
        # Get raw connection for COPY
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            with open(temp_csv_path, 'r', encoding='utf-8') as f:
                cursor.copy_expert(copy_sql, f)
            
            conn.commit()
            logging.info(f"Successfully loaded data to {table_name} using COPY")
            
            # Verify the load
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE source_date = %s", [execution_date])
            loaded_count = cursor.fetchone()[0]
            logging.info(f"Verified: {loaded_count} rows loaded into {table_name} for date {execution_date}")
            
            # Get total row count in table
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            logging.info(f"Total rows in {table_name}: {total_count}")
            
            # Create processing log entry
            log_sql = """
            INSERT INTO data_processing_log 
            (date_processed, date_of_data, files_processed, table_name, total_row_count, column_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_of_data) DO UPDATE SET
                date_processed = EXCLUDED.date_processed,
                files_processed = EXCLUDED.files_processed,
                total_row_count = EXCLUDED.total_row_count,
                column_count = EXCLUDED.column_count
            """
            
            cursor.execute(log_sql, (
                datetime.now(),
                execution_date,
                files_processed,
                table_name,
                total_rows,
                len(columns)
            ))
            
            conn.commit()
            logging.info("Processing log entry created/updated")
            
        except Exception as copy_error:
            conn.rollback()
            logging.error(f"Error during COPY operation: {str(copy_error)}")
            
            # If COPY fails, try INSERT method as fallback
            logging.info("Attempting fallback INSERT method...")
            
            # Read the CSV in chunks to avoid memory issues
            chunk_size = 10000
            total_inserted = 0
            
            for chunk_df in pd.read_csv(temp_csv_path, chunksize=chunk_size):
                # Convert DataFrame to list of tuples
                values = [tuple(row) for row in chunk_df.values]
                
                # Create INSERT statement
                placeholders = ', '.join(['%s'] * len(columns))
                insert_sql = f"""
                INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in columns])})
                VALUES ({placeholders})
                """
                
                # Execute batch insert
                cursor.executemany(insert_sql, values)
                total_inserted += len(values)
                logging.info(f"Inserted {len(values)} rows (total: {total_inserted})")
            
            conn.commit()
            logging.info(f"Fallback INSERT completed: {total_inserted} rows")
            
        finally:
            cursor.close()
            conn.close()
        
    finally:
        # Clean up temporary file
        if os.path.exists(temp_csv_path):
            os.unlink(temp_csv_path)
            logging.info(f"Cleaned up temporary file")

def analyze_bucket_dates(**context):
    """Analyze available dates in S3 bucket"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = Variable.get('S3_BUCKET_NAME')
    
    # List all objects
    objects = s3_hook.list_keys(bucket_name=bucket_name, prefix='')
    
    extracted_dates = {}
    
    for obj_key in objects:
        extracted_date = extract_date_from_filename(obj_key)
        if extracted_date:
            if extracted_date not in extracted_dates:
                extracted_dates[extracted_date] = 0
            extracted_dates[extracted_date] += 1
    
    if extracted_dates:
        sorted_dates = sorted(extracted_dates.keys())
        logging.info(f"Available dates: {min(sorted_dates)} to {max(sorted_dates)}")
        logging.info(f"Total unique dates: {len(sorted_dates)}")
        
        # Log file counts for recent dates
        for date in sorted_dates[-10:]:  # Last 10 dates
            logging.info(f"  {date}: {extracted_dates[date]} files")
    else:
        logging.warning("No recognizable dates found in bucket")
    
    return extracted_dates

# Define tasks
check_existing_data_task = PythonOperator(
    task_id='check_existing_data',
    python_callable=check_existing_data,
    dag=dag
)

check_files_task = PythonOperator(
    task_id='check_s3_files',
    python_callable=check_s3_files,
    dag=dag
)

extract_transform_task = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag
)

# Create tables if they don't exist (modified to include unique constraint)
create_tables_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS data_processing_log (
        id SERIAL PRIMARY KEY,
        date_processed TIMESTAMP,
        date_of_data DATE UNIQUE,
        files_processed INTEGER,
        table_name VARCHAR(100),
        total_row_count INTEGER,
        column_count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

# Optional: Analyze bucket dates task (can be run manually)
analyze_dates_task = PythonOperator(
    task_id='analyze_bucket_dates',
    python_callable=analyze_bucket_dates,
    dag=dag
)

# Define task dependencies
create_tables_task >> check_existing_data_task >> check_files_task >> extract_transform_task >> load_task

# analyze_dates_task is independent and can be run manually