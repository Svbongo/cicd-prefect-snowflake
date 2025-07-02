import os
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple, Union, Dict, Any
from collections import defaultdict
from functools import total_ordering
from prefect import flow, task
import snowflake.connector

# Load environment variables
load_dotenv()

def parse_version(version_str: str) -> Tuple[Union[int, str], ...]:
    version_str = version_str.lstrip('v')
    if not version_str:
        return (0,)
    parts = []
    for part in version_str.split('.'):
        try:
            parts.append(int(part))
        except ValueError:
            parts.append(part)
    return tuple(parts)

@total_ordering
class Version:
    def __init__(self, version_tuple: Tuple[Any, ...]):
        self.version = version_tuple
    def __eq__(self, other): return self.version == other.version
    def __lt__(self, other): return self.version < other.version

def get_sql_files(directory: str) -> Dict[str, List[Tuple[Tuple[Union[int, str], ...], Path]]]:
    sql_files = defaultdict(list)
    base_path = Path(directory)
    for sql_file in base_path.glob('**/*.sql'):
        file_type = sql_file.parent.name
        version_part = sql_file.stem.split('_', 1)[0]
        version = parse_version(version_part)
        sql_files[file_type].append((version, sql_file))
    return sql_files

@task
def run_sql_file(file_path: Path):
    with open(file_path, 'r') as f:
        sql_text = f.read()
    if 'CREATE OR REPLACE PROCEDURE' in sql_text or 'LANGUAGE JAVASCRIPT' in sql_text or 'LANGUAGE SQL' in sql_text:
        sql_commands = [sql_text]
    else:
        sql_commands = [cmd.strip() for cmd in sql_text.split(';') if cmd.strip()]

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

    with snowflake.connector.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(f"USE DATABASE {conn_params['database']}")
            cur.execute(f"USE SCHEMA {conn_params['schema']}")

            for cmd in sql_commands:
                cur.execute(cmd)

@flow
def main_flow():
    sql_files = get_sql_files("sql")
    execution_order = ['DDL', 'Store_Procedures', 'DML', 'Triggers']

    for file_type in execution_order:
        if file_type not in sql_files or not sql_files[file_type]:
            continue
        files_sorted = sorted(
            sql_files[file_type],
            key=lambda x: (Version(x[0]), str(x[1]).lower()),
            reverse=True
        )
        version, file_path = files_sorted[0]
        run_sql_file(file_path)

if __name__ == "__main__":
    main_flow()
