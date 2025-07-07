import os
import argparse
import glob
from prefect import flow, task
import snowflake.connector
from pathlib import Path


@task
def run_sql_file(file_path: Path):
    with open(file_path, 'r') as f:
        sql_text = f.read()

    if 'CREATE OR REPLACE PROCEDURE' in sql_text or 'LANGUAGE JAVASCRIPT' in sql_text or 'LANGUAGE SQL' in sql_text:
        sql_commands = [sql_text]
    else:
        sql_commands = [cmd.strip() for cmd in sql_text.split(';') if cmd.strip()]

    conn_params = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "client_session_keep_alive": True
    }

    print("🚀 Executing file:", file_path.name)
    print("🔍 Connection Parameters:")
    for k, v in conn_params.items():
        print(f"  {k}: {v if v else '❌ MISSING'}")

    try:
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Step 1: Use the database
                print(f"📁 Using database: {conn_params['database']}")
                cur.execute(f"USE DATABASE {conn_params['database']}")

                # Step 2: Verify schema exists
                print(f"🔍 Checking if schema exists: {conn_params['schema']}")
                cur.execute(f"SHOW SCHEMAS LIKE '{conn_params['schema']}'")
                schemas = cur.fetchall()

                if not schemas:
                    raise Exception(f"❌ Schema '{conn_params['schema']}' does not exist in database '{conn_params['database']}'")

                # Step 3: Use the schema
                print(f"📁 Using schema: {conn_params['schema']}")
                cur.execute(f"USE SCHEMA {conn_params['schema']}")

                # Step 4: Execute SQL
                print(f"📦 Running SQL statements from {file_path.name}")
                for cmd in sql_commands:
                    if cmd:  # skip empty
                        cur.execute(cmd)

                print("✅ SQL execution complete for:", file_path.name)

    except Exception as e:
        print("❌ ERROR during SQL execution:")
        print(str(e))
        raise


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
