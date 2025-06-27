from prefect import flow, task, get_run_logger
import snowflake.connector
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional

# Load environment variables from .env file if it exists
load_dotenv()

@task(name="Run SQL File")
def run_sql_file(file_path: str) -> bool:
    """
    Execute SQL commands from a file in Snowflake.
    
    Args:
        file_path: Path to the SQL file to execute
        
    Returns:
        bool: True if successful, False otherwise
    """
    logger = get_run_logger()
    logger.info(f"Executing SQL file: {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            sql = f.read()
            
        if not sql.strip():
            logger.warning(f"SQL file {file_path} is empty")
            return True
            
        # Get connection parameters from environment variables
        conn_params = {
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "DEMO_DB"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA", "DATA_PIPELINE")
        }
        
        logger.info(f"Connecting to Snowflake with params: { {k: '***' if 'password' in k.lower() else v for k, v in conn_params.items()} }")
        
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                # Split SQL by semicolon and execute each statement
                for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
                    try:
                        logger.debug(f"Executing: {stmt[:100]}..." if len(stmt) > 100 else f"Executing: {stmt}")
                        cursor.execute(stmt)
                        logger.info(f"Successfully executed statement")
                    except Exception as e:
                        logger.error(f"Error executing statement: {e}")
                        logger.error(f"Failed statement: {stmt[:500]}..." if len(stmt) > 500 else f"Failed statement: {stmt}")
                        raise
        
        logger.info(f"Successfully executed SQL file: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error executing SQL file {file_path}: {str(e)}")
        raise

@flow(name="Snowflake_CICD_Flow")
def main_flow(sql_dir: str = "sql", file_pattern: str = "*.sql") -> bool:
    """
    Main flow that executes all SQL files in the specified directory.
    
    Args:
        sql_dir: Directory containing SQL files
        file_pattern: Pattern to match SQL files
        
    Returns:
        bool: True if all files were executed successfully
    """
    logger = get_run_logger()
    logger.info("Starting Snowflake CICD Flow")
    
    try:
        # Get all SQL files in the directory
        sql_files = list(Path(sql_dir).glob(file_pattern))
        
        if not sql_files:
            logger.warning(f"No SQL files found in {sql_dir} matching pattern {file_pattern}")
            return True
            
        logger.info(f"Found {len(sql_files)} SQL files to execute")
        
        # Execute each SQL file
        for sql_file in sorted(sql_files):
            run_sql_file(str(sql_file))
            
        logger.info("All SQL files executed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in main flow: {str(e)}")
        raise

if __name__ == "__main__":
    # When run directly, execute the flow with default parameters
    main_flow()
