from prefect import flow, task
import os
import snowflake.connector

# 🔌 Connect to Snowflake
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
def read_sql_file_list(file_path):
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

@task
def categorize_sql_files(sql_files):
    categories = {
        "TABLES": [],
        "VIEWS": [],
        "PROCEDURES": [],
        "TRIGGERS": []
    }

    for path in sql_files:
        parts = path.upper().split('/')
        matched = False
        for part in parts:
            if part in categories:
                categories[part].append(path)
                matched = True
                break
        if not matched:
            print(f"⚠️ Unrecognized path (skipped): {path}")

    for category, files in categories.items():
        print(f"\n📂 Category: {category}")
        if not files:
            print("⚠️ No files found")
        else:
            for f in files:
                print(f"  └─ {f}")

    return categories

@task
def execute_sql_file(file_path):
    try:
        with open(file_path, "r") as f:
            sql = f.read()

        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        print(f"✅ Executed: {file_path}")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Failed: {file_path}")
        print(f"    Reason: {e}")

@flow(name="main-flow")
def main_flow(release_notes: str):
    print(f"📖 Reading SQL file list from: {release_notes}")
    sql_files = read_sql_file_list(release_notes)
    print(f"📂 SQL Files from {release_notes}:\n" + "\n".join(sql_files))

    categories = categorize_sql_files(sql_files)

    for category in ["TABLES", "VIEWS", "PROCEDURES", "TRIGGERS"]:
        print(f"\n🚀 Executing {category} SQL files...")
        for path in categories.get(category, []):
            if os.path.isfile(path):
                execute_sql_file(path)
            else:
                print(f"⚠️ Skipped missing file: {path}")

# 🏁 Entrypoint
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3 or sys.argv[1] not in ["--release-notes"]:
        print("Usage: python main_flow.py --release-notes sorted_sql.txt")
        sys.exit(1)
    main_flow(sys.argv[2])
