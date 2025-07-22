import os
import snowflake.connector
from dotenv import load_dotenv

# Load credentials
load_dotenv()

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=None,
    role=os.getenv("SNOWFLAKE_ROLE")
)

cur = conn.cursor()

# Output folder for DDLs
OUTPUT_FOLDER = "extracted_ddls"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def export_ddl(object_type, database, schema, name, ddl_sql):
    try:
        cur.execute(ddl_sql)
        result = cur.fetchone()
        if result:
            ddl_text = result[0]
            filename = f"{object_type.lower()}__{database}__{schema}__{name}.sql"
            filepath = os.path.join(OUTPUT_FOLDER, filename)
            with open(filepath, "w") as f:
                f.write(ddl_text)
            print(f"📄 Exported {object_type}: {name}")
        else:
            print(f"⚠️ No DDL found for {object_type}: {name}")
    except Exception as e:
        print(f"❌ Failed to export {object_type} {name}: {e}")

def extract_objects():
    cur.execute("SHOW SCHEMAS IN DATABASE DEMO_DB")
    schemas = cur.fetchall()

    for schema_row in schemas:
        schema_name = schema_row[1]
        print(f"\n🔍 Processing schema: {schema_name}")

        # Tables
        cur.execute(f"SHOW TABLES IN DEMO_DB.{schema_name}")
        tables = cur.fetchall()
        print(f"📦 Found {len(tables)} TABLE(s) in {schema_name}")
        for table in tables:
            table_name = table[1]
            export_ddl("TABLE", "DEMO_DB", schema_name, table_name,
                       f"SELECT GET_DDL('TABLE', 'DEMO_DB.{schema_name}.{table_name}')")

        # Views
        cur.execute(f"SHOW VIEWS IN DEMO_DB.{schema_name}")
        views = cur.fetchall()
        print(f"📦 Found {len(views)} VIEW(s) in {schema_name}")
        for view in views:
            view_name = view[1]
            export_ddl("VIEW", "DEMO_DB", schema_name, view_name,
                       f"SELECT GET_DDL('VIEW', 'DEMO_DB.{schema_name}.{view_name}')")

        # Procedures (filtered)
        cur.execute(f"SHOW PROCEDURES IN SCHEMA DEMO_DB.{schema_name}")
        procedures = cur.fetchall()
        print(f"📦 Found {len(procedures)} PROCEDURE(s) in {schema_name}")
        for proc in procedures:
            proc_name = proc[1]
            # Skip system procedures unless needed
            if proc_name.startswith("SYSTEM$"):
                continue
            export_ddl("PROCEDURE", "DEMO_DB", schema_name, proc_name,
                       f"SELECT GET_DDL('PROCEDURE', 'DEMO_DB.{schema_name}.{proc_name}')")

if __name__ == "__main__":
    extract_objects()
    print("\n✅ DDL extraction complete.")
