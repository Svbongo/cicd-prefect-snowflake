from prefect import flow, task
import os

# --- Configurable base SQL folder ---
SQL_FOLDER = "sql"

# --- Task: Read SQL file list from a release note or file list ---
@task
def read_sql_file_list(file_path: str) -> list:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

# --- Task: Categorize SQL files ---
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

# --- Task: Execute SQL files (generalized path resolution) ---
@task
def execute_sql_files(file_paths: list, category: str):
    print(f"🚀 Executing {category} SQL files...")

    # Recursively find all SQL files under SQL_FOLDER
    all_sql_files = []
    for root, dirs, files in os.walk(SQL_FOLDER):
        for file in files:
            if file.endswith(".sql"):
                rel_path = os.path.relpath(os.path.join(root, file), SQL_FOLDER)
                all_sql_files.append(rel_path.replace("\\", "/"))  # Normalize for Windows

    # Try to match each file path from input to real file in project
    for target_file in file_paths:
        normalized_target = target_file.replace("\\", "/").upper()
        matched_path = next((f for f in all_sql_files if f.upper().endswith(normalized_target)), None)

        if matched_path:
            full_path = os.path.join(SQL_FOLDER, matched_path)
            print(f"🔹 Executing {full_path}...")
            # --- Place your Snowflake execution logic here ---
        else:
            print(f"⚠️ File not found: {target_file}")

# --- Main Flow ---
@flow(name="main-flow")
def main_flow(file_path: str):
    print(f"📖 Reading SQL file list from: {file_path}")
    paths = read_sql_file_list(file_path)
    categorized = categorize_sql_files(paths)

    for category, files in categorized.items():
        execute_sql_files(files, category)

# --- CLI Runner ---
if __name__ == "__main__":
    import sys
    input_file = sys.argv[1] if len(sys.argv) > 1 else "modified_sql_files.txt"
    main_flow(input_file)
