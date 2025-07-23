import os
import snowflake.connector
from dotenv import load_dotenv
import subprocess

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    database=os.getenv("SNOWFLAKE_DATABASE")
)

cursor = conn.cursor()
OUTPUT_DIR = "output"

# --- SQL Object Extraction Functions ---

def get_tables(database, schema):
    cursor.execute(f"""
        SELECT TABLE_NAME
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_TYPE = 'BASE TABLE'
    """)
    return [row[0] for row in cursor.fetchall()]

def get_views(database, schema):
    cursor.execute(f"""
        SELECT TABLE_NAME
        FROM {database}.INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = '{schema}'
    """)
    return [row[0] for row in cursor.fetchall()]

def get_procedures(database, schema):
    cursor.execute(f"""
        SELECT PROCEDURE_NAME, ARGUMENT_SIGNATURE
        FROM {database}.INFORMATION_SCHEMA.PROCEDURES
        WHERE PROCEDURE_SCHEMA = '{schema}'
        AND UPPER(PROCEDURE_NAME) NOT LIKE 'SYSTEM$%'
    """)
    return [(row[0], row[1]) for row in cursor.fetchall()]

def export_ddl(object_type, object_signature, schema, output_path):
    try:
        cursor.execute(f"SHOW {object_type}S LIKE '{object_signature.split('(')[0]}' IN SCHEMA {schema}")
        show_result = cursor.fetchone()
        if not show_result:
            print(f"⚠️  {object_type} {object_signature} not found.")
            return

        cursor.execute(f"SELECT GET_DDL('{object_type}', '{schema}.{object_signature}')")
        ddl = cursor.fetchone()[0]

        with open(output_path, "w") as f:
            f.write(ddl)
        print(f"📄 Exported {object_type}: {object_signature}")
    except Exception as e:
        print(f"❌ Failed to export {object_type} {object_signature}: {e}")

# --- Schema Handler ---

def process_schema(database, schema):
    print(f"🔍 Processing schema: {schema}")

    tables = get_tables(database, schema)
    print(f"📦 Found {len(tables)} TABLE(s) in {schema}")
    for table in tables:
        path = os.path.join(OUTPUT_DIR, schema, "Tables", f"{table}.sql")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        export_ddl("TABLE", table, schema, path)

    views = get_views(database, schema)
    print(f"📦 Found {len(views)} VIEW(s) in {schema}")
    for view in views:
        path = os.path.join(OUTPUT_DIR, schema, "Views", f"{view}.sql")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        export_ddl("VIEW", view, schema, path)

    procedures = get_procedures(database, schema)
    print(f"📦 Found {len(procedures)} PROCEDURE(s) in {schema}")
    for proc_name, arg_sig in procedures:
        full_sig = f"{proc_name}{arg_sig}"
        path = os.path.join(OUTPUT_DIR, schema, "Procedures", f"{proc_name}.sql")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        export_ddl("PROCEDURE", full_sig, schema, path)

# --- Main Execution ---

if __name__ == "__main__":
    database = os.getenv("SNOWFLAKE_DATABASE")
    user_schemas = ["DATA_PIPELINE"]

    for schema in user_schemas:
        try:
            process_schema(database, schema)
        except Exception as e:
            print(f"❌ Error processing schema {schema}: {e}")

    # Git auto commit
    try:
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-sync: Snowflake DDLs and Data"], check=True)
        subprocess.run(["git", "push", f"https://x-access-token:{os.getenv('HUB_TOKEN')}@github.com/{os.getenv('GITHUB_REPOSITORY')}.git", "HEAD:main"], check=True)
        print("🚀 Git commit successful.")
    except subprocess.CalledProcessError:
        print("❌ Git operation failed or nothing to commit.")
