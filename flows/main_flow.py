import os
import argparse
import glob
from prefect import flow, task
from utils.snowflake_utils import execute_sql_file

@task
def run_sql_file(filepath):
    print(f"📄 Running: {filepath}")
    try:
        execute_sql_file(filepath)
        print(f"✅ Success: {filepath}")
    except Exception as e:
        print(f"❌ Error in {filepath}: {e}")
        raise e

@flow(name="main-flow")
def main_flow(sql_path=None):
    if sql_path:
        print(f"🔍 Running single file: {sql_path}")
        run_sql_file(sql_path)
        return

    # Updated base paths based on your new structure
    base_dirs = {
        "Tables": "Snowflake/DEMO_DB/DATA_PIPELINE/Tables/*.sql",
        "Procedures": "Snowflake/DEMO_DB/DATA_PIPELINE/Procedures/*.sql",
        "Triggers": "Snowflake/DEMO_DB/DATA_PIPELINE/Triggers/*.sql",
        "Views": "Snowflake/DEMO_DB/DATA_PIPELINE/Views/*.sql"
    }

    for category, pattern in base_dirs.items():
        print(f"\n📂 Processing category: {category}")
        files = sorted(glob.glob(pattern))
        if not files:
            print("⚠️  No files found.")
            continue
        for f in files:
            run_sql_file(f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-path", help="Path to a specific SQL file to execute", required=False)
    args = parser.parse_args()
    main_flow(sql_path=args.sql_path)
