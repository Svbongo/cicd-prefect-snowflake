import os
import argparse
from prefect import flow, task
from utils.snowflake_utils import execute_sql_file

ORDER = ["Tables", "Procedures", "Views", "Triggers"]

@task
def run_sql_file(filepath):
    print(f"📄 Executing: {filepath}")
    try:
        execute_sql_file(filepath)
        print(f"✅ Done: {filepath}")
    except Exception as e:
        print(f"❌ Error in {filepath}: {e}")
        raise e

@flow(name="main-flow")
def main_flow(release_notes_path="release_notes.md"):
    print(f"📜 Reading release notes from: {release_notes_path}")
    
    if not os.path.exists(release_notes_path):
        raise FileNotFoundError(f"{release_notes_path} not found")

    # Read file paths from release notes
    with open(release_notes_path, 'r') as f:
        lines = f.readlines()

    sql_files = [line.strip(" \n*") for line in lines if line.strip().endswith(".sql")]

    # Sort by categories
    categorized = {key: [] for key in ORDER}
    for path in sql_files:
        for category in ORDER:
            if f"/{category}/" in path or f"\\{category}\\" in path:
                categorized[category].append(path)

    # Execute in proper order
    for category in ORDER:
        print(f"\n📂 Category: {category}")
        if not categorized[category]:
            print("⚠️ No files found")
            continue
        for file_path in categorized[category]:
            run_sql_file(file_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-notes", help="Path to release_notes.md", default="release_notes.md")
    args = parser.parse_args()
    main_flow(release_notes_path=args.release_notes)
