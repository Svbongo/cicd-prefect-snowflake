import os
import argparse
import snowflake.connector
from prefect import flow, task

ORDER = ["Tables", "Views", "Procedures", "Triggers"]

# 🔁 Connect to Snowflake and execute SQL file
def execute_sql_file(file_path):
    user = os.environ['SNOWFLAKE_USER']
    password = os.environ['SNOWFLAKE_PASSWORD']
    account = os.environ['SNOWFLAKE_ACCOUNT']
    database = os.environ['SNOWFLAKE_DATABASE']
    schema = os.environ['SNOWFLAKE_SCHEMA']
    warehouse = os.environ['SNOWFLAKE_WAREHOUSE']

    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    cs = ctx.cursor()

    try:
        with open(file_path, 'r') as f:
            sql_script = f.read()

        # 🛠 If it's a procedure, run as one block
        if file_path.lower().endswith(".sql") and "create or replace procedure" in sql_script.lower():
            print(f"⚙️ Executing full procedure file: {file_path}")
            cs.execute(sql_script)
        else:
            print(f"📄 Executing statements from: {file_path}")
            statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
            for stmt in statements:
                print(f"➡️ {stmt[:60]}{'...' if len(stmt) > 60 else ''}")
                cs.execute(stmt)

    except Exception as e:
        print(f"❌ Failed: {e}")
        raise e
    finally:
        cs.close()
        ctx.close()


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
def main_flow(release_notes_path="sorted_sql.txt"):
    print(f"📜 Reading SQL file list from: {release_notes_path}")

    if not os.path.exists(release_notes_path):
        raise FileNotFoundError(f"{release_notes_path} not found")

    with open(release_notes_path, 'r') as f:
        sql_files = [
            line.strip().upper()
            for line in f
            if line.strip().lower().endswith(".sql")
        ]

    # 🧾 Log the full list
    print("🧾 SQL Files Found:")
    for path in sql_files:
        print(f" - {path}")

    categorized = {key: [] for key in ORDER}

    for path in sql_files:
        for category in ORDER:
            if f"/{category.upper()}/" in path or f"\\{category.upper()}\\" in path:
                categorized[category].append(path)

    # 🔁 Execute in category order
    for category in ORDER:
        print(f"\n📂 Category: {category}")
        if not categorized[category]:
            print("⚠️ No files found")
            continue
        for file_path in categorized[category]:
            print(f"✅ Will execute: {file_path}")
            run_sql_file(file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-notes", help="Path to sorted SQL file list", default="sorted_sql.txt")
    args = parser.parse_args()
    main_flow(release_notes_path=args.release_notes)
