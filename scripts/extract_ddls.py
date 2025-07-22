import snowflake.connector
import os
from pathlib import Path

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="INFORMATION_SCHEMA",
    role=os.getenv("SNOWFLAKE_ROLE"),
)
cursor = conn.cursor()

# Set output directory
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

def is_user_defined_proc(proc_name):
    """Filter out system or observability-related procedures"""
    return not (
        proc_name.startswith("SYSTEM$")
        or "OBSERVABILITY" in proc_name.upper()
        or proc_name.startswith("SNOWFLAKE.")
    )

def export_table_ddl(schema, table):
    cursor.execute(f"SHOW CREATE TABLE {schema}.{table}")
    ddl = cursor.fetchone()[1]
    out_path = OUTPUT_DIR / schema / "Tables"
    out_path.mkdir(parents=True, exist_ok=True)
    with open(out_path / f"{table}.sql", "w") as f:
        f.write(ddl)

def export_procedure_ddl(schema, proc_name):
    try:
        cursor.execute(f"SHOW CREATE PROCEDURE {schema}.{proc_name}")
        ddl = cursor.fetchone()[1]
        out_path = OUTPUT_DIR / schema / "Procedures"
        out_path.mkdir(parents=True, exist_ok=True)
        with open(out_path / f"{proc_name}.sql", "w") as f:
            f.write(ddl)
    except Exception as e:
        print(f"❌ Failed to export PROCEDURE {proc_name}: {e}")

def get_tables(schema):
    cursor.execute(f"SELECT TABLE_NAME FROM {schema}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    return [row[0] for row in cursor.fetchall()]

def get_procedures(schema):
    cursor.execute(f"""
        SELECT PROCEDURE_NAME
        FROM {schema}.INFORMATION_SCHEMA.PROCEDURES
        WHERE UPPER(PROCEDURE_NAME) NOT LIKE 'SYSTEM$%'
        AND UPPER(PROCEDURE_NAME) NOT LIKE '%OBSERVABILITY%'
        AND PROCEDURE_CATALOG != 'SNOWFLAKE'
    """)
    return [row[0] for row in cursor.fetchall()]

def process_schema(schema):
    print(f"🔍 Processing schema: {schema}")
    tables = get_tables(schema)
    print(f"📦 Found {len(tables)} TABLE(s) in {schema}")
    for table in tables:
        print(f"📄 Exported TABLE: {table}")
        export_table_ddl(schema, table)

    procs = get_procedures(schema)
    print(f"📦 Found {len(procs)} PROCEDURE(s) in {schema}")
    for proc in procs:
        print(f"📄 Exporting PROCEDURE: {proc}")
        export_procedure_ddl(schema, proc)

# Main
user_schemas = ["DATA_PIPELINE"]
for schema in user_schemas:
    process_schema(schema)

cursor.close()
conn.close()
print("✅ DDL extraction complete.")
