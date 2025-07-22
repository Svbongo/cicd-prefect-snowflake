import os
import snowflake.connector
import subprocess
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Snowflake connection info
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

# Output directory
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

excluded_schemas = {"INFORMATION_SCHEMA", "PUBLIC", "STAGING"}

cursor = conn.cursor()
cursor.execute("SHOW SCHEMAS")
schemas = [row[1] for row in cursor.fetchall() if row[1] not in excluded_schemas]

for schema in schemas:
    print(f"\n🔍 Processing schema: {schema}")
    schema_dir = os.path.join(OUTPUT_DIR, schema)
    os.makedirs(schema_dir, exist_ok=True)

    cursor.execute(f"SHOW TABLES IN SCHEMA {schema}")
    tables = [row[1] for row in cursor.fetchall()]
    print(f"📦 Found {len(tables)} TABLE(s) in {schema}")

    for table in tables:
        cursor.execute(f"SELECT GET_DDL('TABLE', '{schema}.{table}')")
        ddl = cursor.fetchone()[0]
        print(f"📄 Exported TABLE: {table}")
        with open(os.path.join(schema_dir, f"{table}.sql"), "w") as f:
            f.write(ddl)

    # Views
    cursor.execute(f"SHOW VIEWS IN SCHEMA {schema}")
    views = [row[1] for row in cursor.fetchall()]
    print(f"📦 Found {len(views)} VIEW(s) in {schema}")
    for view in views:
        try:
            cursor.execute(f"SELECT GET_DDL('VIEW', '{schema}.{view}')")
            ddl = cursor.fetchone()[0]
            print(f"📄 Exported VIEW: {view}")
            with open(os.path.join(schema_dir, f"{view}_view.sql"), "w") as f:
                f.write(ddl)
        except Exception as e:
            print(f"❌ Failed to export VIEW {view}: {e}")

    # Procedures
    cursor.execute(f"SHOW PROCEDURES IN SCHEMA {schema}")
    procedures = cursor.fetchall()
    print(f"📦 Found {len(procedures)} PROCEDURE(s) in {schema}")

    for proc in procedures:
        proc_name = proc[1]
        signature = proc[9]  # Fully qualified signature

        if proc_name.startswith("SYSTEM$"):
            continue  # Skip system procedures

        try:
            print(f"📄 Exporting PROCEDURE: {proc_name} (no signature)")
            cursor.execute(f"SELECT GET_DDL('PROCEDURE', '{schema}.{proc_name}')")
            ddl = cursor.fetchone()[0]
        except Exception:
            try:
                print(f"🔁 Retrying PROCEDURE with signature: {signature}")
                cursor.execute(f"SELECT GET_DDL('PROCEDURE', '{schema}.{signature}')")
                ddl = cursor.fetchone()[0]
            except Exception as e:
                print(f"❌ Failed to export PROCEDURE {proc_name}: {e}")
                continue

        filename = f"{proc_name}_proc.sql"
        with open(os.path.join(schema_dir, filename), "w") as f:
            f.write(ddl)

# Git commit
try:
    subprocess.run(["git", "add", OUTPUT_DIR], check=True)
    subprocess.run(["git", "commit", "-m", "Auto-sync: Snowflake DDLs and Data"], check=True)
    print("🚀 Git commit successful.")
except subprocess.CalledProcessError:
    print("ℹ️ No changes to commit.")

print("\n✅ DDL + Data extraction complete.")
