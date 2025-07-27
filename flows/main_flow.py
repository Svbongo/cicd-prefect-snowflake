from prefect import flow, task
import argparse
from pathlib import Path
import os
import snowflake.connector

ROOT_DIR = Path(__file__).resolve().parent.parent

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        autocommit=True  # ✅ Enables commits automatically
    )

@task
def read_sql_file_list(file_path: str) -> list:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

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
    """Executes SQL files against Snowflake."""
    conn = get_snowflake_connection()

    try:
        for sql_file in sql_file_list:
            normalized_path = ROOT_DIR / sql_file
            if not normalized_path.exists():
                print(f"⚠️ File not found: {normalized_path}")
                continue

            print(f"\n📂 Running: {sql_file}")
            try:
                with conn.cursor() as cur, open(normalized_path, "r") as f:
                    sql = f.read()

                    # Log current DB/schema
                    cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                    db, schema = cur.fetchone()
                    print(f"🔍 Using DB: {db} | Schema: {schema}")

                    if "create or replace procedure" in sql.lower():
                        print("🧩 Detected stored procedure — executing as single block.")
                        cur.execute(sql)
                    else:
                        statements = [stmt.strip() for stmt in sql.strip().split(";") if stmt.strip()]
                        for idx, stmt in enumerate(statements):
                            if not stmt.startswith("--"):
                                print(f"🔹 Executing statement {idx+1}: {stmt[:60]}...")
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

    # Flatten and execute all categorized files
    for files in categorized.values():
        if files:
            execute_sql_files(files)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-notes", type=str, default="sorted_sql.txt",
        help="Path to the sorted SQL file list"
    )
    args = parser.parse_args()
    main_flow(args.release_notes)
