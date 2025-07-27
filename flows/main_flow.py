from prefect import flow, task
from pathlib import Path

# File paths
MODIFIED_SQL_FILE = "modified_sql_files.txt"
SORTED_SQL_FILE = "sorted_sql.txt"

# Category mapping
CATEGORIES = {
    "tables": "TABLES",
    "views": "VIEWS",
    "procedures": "PROCEDURES",
    "triggers": "TRIGGERS"
}

@task
def read_sql_file_list(file_path: str) -> list:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

@task
def categorize_sql_files(paths: list) -> dict:
    categorized = {
        "TABLES": [],
        "VIEWS": [],
        "PROCEDURES": [],
        "TRIGGERS": [],
        "OTHERS": []
    }

    for raw_path in paths:
        lower_path = raw_path.lower()
        path_obj = Path(raw_path)
        matched = False

        for keyword, category in CATEGORIES.items():
            if keyword in lower_path:
                categorized[category].append(str(path_obj))
                matched = True
                break

        if not matched:
            categorized["OTHERS"].append(str(path_obj))

    return categorized

@task
def write_sorted_paths(categorized: dict, output_path: str):
    final_order = (
        categorized["TABLES"] +
        categorized["VIEWS"] +
        categorized["PROCEDURES"] +
        categorized["TRIGGERS"] +
        categorized["OTHERS"]
    )
    with open(output_path, "w") as f:
        for p in final_order:
            f.write(p + "\n")
    print(f"✅ Sorted SQL paths written to: {output_path}")

@flow(name="main-flow")
def main_flow():
    print(f"📖 Reading SQL file list from: {MODIFIED_SQL_FILE}")
    paths = read_sql_file_list(MODIFIED_SQL_FILE)

    print("📂 SQL Files from modified_sql_files.txt:")
    for path in paths:
        print("  └─", path)

    categorized = categorize_sql_files(paths)

    for category, files in categorized.items():
        print(f"\n📂 Category: {category}")
        if files:
            for f in files:
                print("  └─", f)
        else:
            print("⚠️ No files found")

    write_sorted_paths(categorized, SORTED_SQL_FILE)

    print("\n🚀 Proceeding to execution using sorted_sql.txt...")
    # You can call another function here to execute the sorted files if needed

if __name__ == "__main__":
    main_flow()
