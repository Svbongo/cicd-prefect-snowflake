from prefect import flow, task
import os
import argparse
from pathlib import Path    

SQL_FOLDER = "sql"

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
def execute_sql_files(sql_file_list, file_type):
    print(f"🚀 Executing {file_type.upper()} SQL files...")

    for sql_file in sql_file_list:
        if file_type.upper() not in sql_file.upper():
            continue

        # Normalize path (avoid uppercased 'SNOWFLAKE' mismatch)
        normalized_path = Path(sql_file)
        if not normalized_path.exists():
            print(f"⚠️ File not found: {normalized_path}")
            continue

        try:
            with open(normalized_path, "r") as file:
                sql_commands = file.read()
                print(f"✅ Executing {sql_file}...")
                snowflake_cursor.execute(sql_commands)
        except Exception as e:
            print(f"❌ Error executing {sql_file}: {e}")

@flow(name="main-flow")
def main_flow(file_path: str):
    print(f"📖 Reading SQL file list from: {file_path}")
    paths = read_sql_file_list(file_path)
    categorized = categorize_sql_files(paths)

    for category, files in categorized.items():
        execute_sql_files(files, category)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-notes", type=str, default="modified_sql_files.txt",
        help="Path to the release notes or SQL file list"
    )
    args = parser.parse_args()
    main_flow(args.release_notes)
