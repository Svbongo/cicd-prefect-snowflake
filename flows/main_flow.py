from prefect import flow, task
from pathlib import Path
import os
import snowflake.connector

# Adjust to the root of your repo where 'Snowflake/...' exists
ROOT_DIR = Path(__file__).resolve().parent.parent

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        autocommit=True
    )

@task
def read_sql_file_list(file_path: str) -> list:
    with open(file_path, "r") as f:
        sql_paths = [line.strip() for line in f if line.strip()]
    print(f"✅ Loaded SQL paths from release notes: {sql_paths}")
    return sql_paths

@task
def categorize_sql_files(sql_file_paths: list) -> dict:
    categories = {"TABLES": [], "VIEWS": [], "PROCEDURES": [], "TRIGGERS": []}
    for path in sql_file_paths:
        upper_path = path.upper()
        if "TABLES" in upper_path:
            categories["TABLES"].append(path)
        elif "VIEWS" in upper_path:
            categories["VIEWS"].append(path)
        elif "PROCEDURES" in upper_path:
            categories["PROCEDURES"].append(path)
        elif "TRIGGERS" in upper_path:
            categories["TRIGGERS"].append(path)
    return categories

@task
def execute_sql_files(sql_file_list: list):
    conn = get_snowflake_connection()

    try:
        for sql_file in sql_file_list:
            normalized_path = ROOT_DIR / sql_file

            print(f"\n🔍 Checking for file: {normalized_path}")
            if not normalized_path.exists():
                print(f"❌ File not found: {normalized_path}")
                continue

            print(f"\n📂 Running: {sql_file}")
            try:
                with conn.cursor() as cur, open(normalized_path, "r") as f:
                    cur.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
                    cur.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}")

                    sql = f.read()

                    # Run full block for procedures
                    if "create or replace procedure" in sql.lower():
                        print("🧩 Detected stored procedure — executing as single block.")
                        cur.execute(sql)
                    else:
                        import re
                        statements = [stmt.strip() for stmt in re.split(r';\s*\n', sql) if stmt.strip()]
                        for idx, stmt in enumerate(statements):
                            # Don't skip statements that contain SQL + comments
                            if stmt.lower().startswith("use") or "create" in stmt.lower() or "insert" in stmt.lower():
                                print(f"🔹 Executing statement {idx+1}: {stmt[:60]}...")
                                cur.execute(stmt)
                            else:
                                print(f"⚠️ Skipping non-SQL or unsupported line: {stmt[:60]}")
                                cur.execute(stmt)

                    print(f"✅ Success: {sql_file}")
            except Exception as e:
                print(f"❌ Error in {sql_file}:\n{e}")
    finally:
        conn.close()
        print("✅ Snowflake connection closed.")

@flow(name="main-flow")
def main_flow(file_path: str):
    sql_paths = read_sql_file_list(file_path)
    categorized = categorize_sql_files(sql_paths)

    for category, files in categorized.items():
        if files:
            print(f"\n🚀 Executing {category} files...")
            execute_sql_files(files)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-notes", type=str, default="sorted_sql.txt",
        help="Path to the sorted SQL file list"
    )
    args = parser.parse_args()
    main_flow(args.release_notes)
