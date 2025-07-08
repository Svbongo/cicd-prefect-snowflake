import os
import argparse
import snowflake.connector
from prefect import flow, task

ORDER = ["Tables", "Procedures", "Views", "Triggers"]

# 🔁 Previously in utils.snowflake_utils
def execute_sql_file(file_path):
    with open(file_path, 'r') as f:
        sql_statements = f.read().split(';')

    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        role=os.environ['SNOWFLAKE_ROLE']
    )
    cursor = conn.cursor()
    try:
        for stmt in sql_statements:
            stmt = stmt.strip()
            if stmt:
                print(f"⚙️ Running: {stmt[:50]}...")
                cursor.execute(stmt)
    finally:
        cursor.close()
        conn.close()

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
