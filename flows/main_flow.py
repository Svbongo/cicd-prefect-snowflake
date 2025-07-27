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
def execute_sql_files(sql_file_list: list, file_type: str):
    print(f"\n🚀 Executing {file_type.upper()} SQL files...")
    conn = get_snowflake_connection()

    try:
        for sql_file in sql_file_list:
            normalized_path = ROOT_DIR / sql_file
            if not normalized_path.exists():
                print(f"⚠️ File not found: {normalized_path}")
                continue

            print(f"📂 Running: {sql_file}")
            try:
                with conn.cursor() as cur, open(normalized_path, "r") as f:
                    sql = f.read()

                    if file_type.upper() == "PROCEDURES":
                        # Execute entire block for procedure
                        cur.execute_string(sql)
                    else:
                        # Execute statements one by one
                        for stmt in sql.strip().split(";"):
                            stmt = stmt.strip()
                            if stmt and not stmt.startswith("--"):
                                cur.execute(stmt)

                print(f"✅ Success: {sql_file}")
            except Exception as e:
                print(f"❌ Error in {sql_file}: {e}")
    finally:
        conn.close()
        print("✅ Snowflake connection closed.")

@flow(name="main-flow")
def main_flow(sql_file_list: list, file_type: str):
    print(f"\n🚀 Executing {file_type.upper()} SQL files...")
    conn = get_snowflake_connection()

    try:
        for sql_file in sql_file_list:
            normalized_path = ROOT_DIR / sql_file
            if not normalized_path.exists():
                print(f"⚠️ File not found: {normalized_path}")
                continue

            print(f"📂 Running: {sql_file}")
            try:
                with conn.cursor() as cur, open(normalized_path, "r") as f:
                    sql = f.read()

                    if file_type.upper() == "PROCEDURES":
                        cur.execute_string(sql)
                    else:
                        for stmt in sql.strip().split(";"):
                            stmt = stmt.strip()
                            if stmt and not stmt.startswith("--"):
                                cur.execute(stmt)

                print(f"✅ Success: {sql_file}")
            except Exception as e:
                print(f"❌ Error in {sql_file}: {e}")
    finally:
        conn.close()
        print("✅ Snowflake connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-notes", type=str, default="sorted_sql.txt",
        help="Path to the sorted SQL file list"
    )
    args = parser.parse_args()

    # Load file list
    file_list = read_sql_file_list.fn(args.release_notes)

    if not file_list:
        print("❌ No SQL files found in the release notes.")
        exit(1)

    # Infer file_type from path of the first file
    upper_path = file_list[0].upper()
    if "PROCEDURES" in upper_path:
        file_type = "PROCEDURES"
    elif "TABLES" in upper_path:
        file_type = "TABLES"
    elif "VIEWS" in upper_path:
        file_type = "VIEWS"
    elif "TRIGGERS" in upper_path:
        file_type = "TRIGGERS"
    else:
        print("❌ Could not infer file type from file path.")
        exit(1)

    # Run the Prefect flow
    main_flow(sql_file_list=file_list, file_type=file_type)
