from prefect import flow, task, get_run_logger
import snowflake.connector
import os
import re
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional, Tuple, Dict, Union, Any
from collections import defaultdict
from functools import total_ordering

# Load environment variables from .env file if it exists
load_dotenv()

def parse_version(version_str: str) -> Tuple[Union[int, str], ...]:
    """
    Parse version string (e.g., '1.2.3') into a tuple of integers/strings.
    Handles both version formats: '1.2.3', 'v1.2.3', and decimal versions like '1.1'.
    
    Returns:
        Tuple where each element represents a part of the version number.
        For '1.2.3' returns (1, 2, 3)
        For '1.1' returns (1, 1)
        For '2' returns (2,)
    """
    # Remove 'v' prefix if present
    version_str = version_str.lstrip('v')
    
    # Handle empty string
    if not version_str:
        return (0,)
        
    try:
        # Split by dot and convert to integers where possible
        parts = []
        for part in version_str.split('.'):
            try:
                parts.append(int(part))
            except ValueError:
                parts.append(part)  # Keep as string if not a number
        return tuple(parts)
    except Exception:
        return (0,)  # Default version for invalid version strings

@total_ordering
class Version:
    """Helper class for comparing version tuples"""
    def __init__(self, version_tuple: Tuple[Any, ...]):
        self.version = version_tuple
        
    def __eq__(self, other):
        if not isinstance(other, Version):
            return NotImplemented
        return self.version == other.version
        
    def __lt__(self, other):
        if not isinstance(other, Version):
            return NotImplemented
        return self.version < other.version

def get_sql_files(directory: str) -> Dict[str, List[Tuple[Tuple[Union[int, str], ...], Path]]]:
    """
    Get all SQL files in the directory, organized by file type (DDL, DML, etc.).
    
    Expected file naming pattern: 
    - DDL/1_name.sql, DDL/2_name.sql, etc.
    - DML/1_name.sql, DML/2_name.sql, etc.
    
    Returns:
        Dict mapping file type to list of (version, file_path) tuples
    """
    sql_files = defaultdict(list)
    path = Path(directory)
    
    # Pattern to match version numbers at the start of filenames (supports decimals like 1.1, 2.0, etc.)
    version_pattern = re.compile(r'^(\d+(?:\.\d+)*)(?:_|$)', re.IGNORECASE)
    
    for sql_file in path.rglob('*.sql'):
        # Get the relative path from the SQL directory
        rel_path = sql_file.relative_to(directory)
        
        # The first part of the path is the file type (DDL, DML, etc.)
        if len(rel_path.parts) < 2:
            continue  # Skip files in the root directory
            
        file_type = rel_path.parts[0]
        file_stem = sql_file.stem
        
        # Extract version from filename (number at the start)
        match = version_pattern.match(file_stem)
        if match:
            version = parse_version(match.group(1))
            sql_files[file_type].append((version, sql_file))
        else:
            # If no version number found, use 0 as default version
            sql_files[file_type].append(((0,), sql_file))
    
    return sql_files

@task(name="Run SQL File")
def run_sql_file(file_path: Path) -> bool:
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
        with open(file_path, 'r', encoding='utf-8') as f:
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
        
        logger.info(f"Connecting to Snowflake with params: {{k: '***' if 'password' in k.lower() else v for k, v in conn_params.items()}}")
        
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                # Split SQL by semicolon and execute each statement
                for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
                    try:
                        logger.debug(f"Executing: {stmt[:100]}..." if len(stmt) > 100 else f"Executing: {stmt}")
                        cursor.execute(stmt)
                    except Exception as e:
                        logger.error(f"Error executing statement: {e}")
                        logger.error(f"Failed statement: {stmt}")
                        return False
        
        logger.info(f"Successfully executed {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error executing {file_path}: {str(e)}")
        return False

@flow(name="Snowflake SQL Deployment")
def main_flow(sql_dir: str = "sql"):
    """
    Main flow that executes SQL files in version order from the specified directory.
    
    The function processes files in the following order:
    1. DDL files (by version number, then alphabetically)
    2. DML files (by version number, then alphabetically)
    3. Store_Procedures files (by version number, then alphabetically)
    4. Triggers files (by version number, then alphabetically)
    
    Args:
        sql_dir: Directory containing SQL files in subdirectories by type
        
    Returns:
        bool: True if all files were executed successfully, False otherwise
    """
    logger = get_run_logger()
    logger.info("Starting versioned SQL deployment flow")
    
    # Get all SQL files organized by type and version
    sql_files = get_sql_files(sql_dir)
    
    if not sql_files:
        logger.warning(f"No SQL files found in {sql_dir}")
        return True
    
    # Log found files
    logger.info("\nFound the following SQL files:")
    for file_type, files in sql_files.items():
        logger.info(f"\n{file_type}:")
        for version, path in sorted(files, key=lambda x: x[0]):
            logger.info(f"  v{'.'.join(map(str, version))}: {path}")
    
    # Define the execution order of file types
    execution_order = ['DDL', 'Store_Procedures','DML', 'Triggers']
    
    all_success = True
    
    for file_type in execution_order:
        if file_type not in sql_files or not sql_files[file_type]:
            logger.info(f"\nNo {file_type} files to process")
            continue
            
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing {file_type} files")
        logger.info(f"{'='*50}")
        
        # Sort files by version (lowest first), then filename
        files_sorted = sorted(
            sql_files[file_type],
            key=lambda x: (Version(x[0]), str(x[1]).lower())
        )

        # Only take the top-most file
        version, file_path = files_sorted[0]
        version_str = '.'.join(map(str, version)) if version != (0,) else 'unversioned'
        logger.info(f"\n⚡ Executing top {file_type} v{version_str}: {file_path.name}")
        
        success = run_sql_file(file_path)
        if not success:
            logger.error(f"❌ Failed to execute {file_path}")
            all_success = False

        
    return all_success

if __name__ == "__main__":
    # When run directly, execute the flow with default parameters
    main_flow()
