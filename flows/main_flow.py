import logging
import sys
from prefect import flow, task, get_run_logger
import snowflake.connector
import os
import re
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional, Tuple, Dict, Union, Any
from collections import defaultdict
from functools import total_ordering

# Configure root logger to capture all output
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root_logger.addHandler(handler)

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
    
    Returns:
        Dict mapping file type to list of (version, file_path) tuples
    """
    sql_files = defaultdict(list)
    base_path = Path(directory)
    
    if not base_path.exists():
        return {}
    
    # Look for SQL files in all subdirectories
    for sql_file in base_path.glob('**/*.sql'):
        # Get the parent directory name as the file type
        file_type = sql_file.parent.name
        
        # Extract version from filename (e.g., "1_script.sql" -> "1")
        version_part = sql_file.stem.split('_', 1)[0]
        version = parse_version(version_part)
        
        sql_files[file_type].append((version, sql_file))
    
    return sql_files

@task(name="Run SQL File")
def run_sql_file(file_path: Path) -> bool:
    """Execute SQL commands from a file with proper error handling and logging."""
    logger = get_run_logger()
    logger.info("\n" + "="*80)
    logger.info(f"📄 EXECUTING SQL FILE: {file_path}")
    logger.info("="*80)
    
    try:
        # Read and log the file content
        with open(file_path, 'r') as f:
            file_content = f.read()
            logger.info(f"\nFile content:\n{file_content}")
            sql_commands = [cmd.strip() for cmd in file_content.split(';') if cmd.strip()]
        
        # Get connection parameters from environment variables
        conn_params = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "client_session_keep_alive": True
        }
        
        # Log connection info (without password)
        conn_info = {k: v for k, v in conn_params.items() if k != "password"}
        logger.info("\n🔗 Connection Parameters:")
        for k, v in conn_info.items():
            logger.info(f"   {k}: {v}")
        
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Log current session info
                logger.info("\n🔍 Session Information:")
                try:
                    cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_SESSION()")
                    account, region, session = cur.fetchone()
                    logger.info(f"   Account: {account}")
                    logger.info(f"   Region: {region}")
                    logger.info(f"   Session: {session}")
                except Exception as e:
                    logger.warning(f"Could not get session info: {e}")
                
                # Get current database and schema
                cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                db, schema = cur.fetchone()
                logger.info(f"   Database: {db}")
                logger.info(f"   Schema: {schema}")
                
                # List all stored procedures in the current schema
                try:
                    logger.info("\n📋 Available procedures in current schema:")
                    cur.execute("""
                        SELECT 
                            procedure_name,
                            procedure_definition
                        FROM information_schema.procedures 
                        WHERE procedure_schema = CURRENT_SCHEMA()
                    """)
                    procedures = cur.fetchall()
                    if procedures:
                        for proc in procedures:
                            logger.info(f"   - {proc[0]}")
                            logger.info(f"     {proc[1][:200]}...")
                    else:
                        logger.info("   No procedures found in current schema")
                except Exception as e:
                    logger.error(f"❌ Error listing procedures: {e}")
                
                # Execute each command
                logger.info("\n🚀 Executing SQL commands:")
                for i, cmd in enumerate(sql_commands, 1):
                    if not cmd.strip():
                        continue
                        
                    logger.info("\n" + "-"*60)
                    logger.info(f"💻 Command {i}/{len(sql_commands)}:")
                    logger.info(f"{cmd}")
                    
                    try:
                        cur.execute(cmd)
                        logger.info("✅ Command executed successfully")
                        
                        # Try to fetch results if any
                        try:
                            results = cur.fetchall()
                            if results:
                                logger.info("📊 Results:")
                                for row in results:
                                    logger.info(f"   {row}")
                        except Exception as fetch_error:
                            # It's normal for some commands to not return results
                            logger.debug(f"No results to fetch: {fetch_error}")
                            
                    except Exception as e:
                        logger.error(f"❌ Error executing command: {e}")
                        logger.error(f"Full command: {cmd}")
                        logger.error(f"Error type: {type(e).__name__}")
                        logger.error(f"Error details: {str(e)}")
                        return False
        
        logger.info("\n✅ " + "="*50)
        logger.info(f"✅ Successfully executed {file_path}")
        logger.info("="*50 + "\n")
        return True
        
    except Exception as e:
        logger.error("\n❌ " + "="*50)
        logger.error(f"❌ ERROR in run_sql_file: {e}")
        logger.error(f"❌ Error type: {type(e).__name__}")
        import traceback
        logger.error("\nTraceback:" + "\n" + "\n".join(traceback.format_exc().splitlines()))
        logger.error("="*50 + "\n")
        return False

@flow(name="Snowflake SQL Deployment")
def main_flow():
    """Main flow to execute SQL files in version order."""
    logger = get_run_logger()
    logger.info("\n" + "="*50)
    logger.info("🚀 Starting versioned SQL deployment flow")
    
    # Find all SQL files
    sql_files = get_sql_files("sql")
    
    # Log found files
    logger.info("\n📁 Found the following SQL files:")
    for file_type, files in sql_files.items():
        logger.info(f"\n{file_type}:")
        for version, path in sorted(files, key=lambda x: x[0]):
            version_str = '.'.join(map(str, version)) if version != (0,) else 'unversioned'
            logger.info(f"  v{version_str}: {path}")
    
    # Define the execution order of file types
    execution_order = ['DDL', 'Store_Procedures', 'DML', 'Triggers']
    
    all_success = True
    
    for file_type in execution_order:
        if file_type not in sql_files or not sql_files[file_type]:
            logger.info(f"\nℹ️  No {file_type} files to process")
            continue
            
        logger.info("\n" + "="*50)
        logger.info(f"🔄 Processing {file_type} files")
        logger.info("="*50)
        
        # Sort files by version (lowest first), then filename
        files_sorted = sorted(
            sql_files[file_type],
            key=lambda x: (Version(x[0]), str(x[1]).lower())
        )

        # Process all files in this directory
        for version, file_path in files_sorted:
            version_str = '.'.join(map(str, version)) if version != (0,) else 'unversioned'
            logger.info(f"\n⚡ Executing {file_type} v{version_str}: {file_path.name}")
            
            try:
                success = run_sql_file(file_path)
                if not success:
                    logger.error(f"❌ Failed to execute {file_path}")
                    all_success = False
                    # Continue with other files even if one fails
                    continue
                logger.info(f"✅ Successfully executed {file_path}")
            except Exception as e:
                logger.error(f"❌ Unexpected error executing {file_path}: {str(e)}")
                all_success = False
                continue
    
    # Final status
    logger.info("\n" + "="*50)
    if all_success:
        logger.info("✨ All files processed successfully!")
    else:
        logger.error("❌ Some files failed to execute. Check the logs above for details.")
    
    return all_success

if __name__ == "__main__":
    # When run directly, execute the flow with default parameters
    main_flow()
