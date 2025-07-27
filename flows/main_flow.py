from prefect import flow, task
import argparse
from pathlib import Path
import os
import snowflake.connector

# Get the repo root
ROOT_DIR = Path(__file__).resolve().parent.parent

# ✅ Define the connection function
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

@task
def read_sql_file_list(file_path: str) -> list:
    """Reads SQL file list from a file"""
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

@task
def categorize_sql_files(sql_file_paths: list) -> dict:
    """Categorizes SQL file paths into TABLES, VIEWS, PROCEDURES, TRIGGERS"""
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
def execute_sql_files(sql_file_list, file_type):
    print(f"\n🚀 Executing {file_type.upper()} SQL files...")
    conn = get_snowflake_connection()
    try:
        for sql_file in sql_file_list:
            normalized_path = ROOT_DIR / sql_file
            if not normalized_path.exists():
                print(f"⚠️ File not found: {normalized_path}")
                continue

            with conn.cursor() as cur, open(normalized_path, "r") as file:
                sql = file.read()
                try:
                    print(f"📂 Running: {sql_file}")
                    cur.execute(sql, multiple_statements=True)
                    print(f"✅ Success: {sql_file}")
                except Exception as e:
                    print(f"❌ Error in {sql_file}: {e}")
    finally:
        conn.close()
        print("✅ Snowflake connection closed.")

@flow(name="main-flow")
def main_flow(file_path: str):
    sql_files = read_sql_file_list(file_path)
    categorized = categorize_sql_files(sql_files)

    for category in ["TABLES", "VIEWS", "PROCEDURES", "TRIGGERS"]:
        execute_sql_files(categorized[category], category)

# Entry point for CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-notes", type=str, default="sorted_sql.txt",
        help="Path to the release notes or SQL file list"
    )
    args = parser.parse_args()
    main_flow(args.release_notes)
