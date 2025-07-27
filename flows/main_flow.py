from prefect import flow, task
import os
import argparse

# --- Configurable SQL folder path ---
SQL_FOLDER = "."  # or "" to treat paths as relative from repo root


# --- Task to read SQL file list ---
@task
def read_sql_file_list(file_path: str) -> list:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

# --- Task to categorize SQL files by type ---
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

# --- Task to execute SQL files ---
@task
def execute_sql_files(file_paths: list, category: str):
    print(f"🚀 Executing {category} SQL files...")
    for file_path in file_paths:
        full_path = file_path  # since the file_path is already a full relative path

        if os.path.exists(full_path):
            print(f"🔹 Executing {full_path}...")
            # Integrate Snowflake execution here
        else:
            print(f"⚠️ File not found: {full_path}")

# --- Main flow ---
@flow(name="main-flow")
def main_flow(file_path: str):
    print(f"📖 Reading SQL file list from: {file_path}")
    paths = read_sql_file_list(file_path)
    categorized = categorize_sql_files(paths)

    for category, files in categorized.items():
        execute_sql_files(files, category)

# --- CLI runner ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Prefect SQL deployment flow.")
    parser.add_argument("--release-notes", required=True, help="Path to sorted SQL file list")

    args = parser.parse_args()
    main_flow(args.release_notes)
