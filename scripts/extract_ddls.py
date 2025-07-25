import os
import snowflake.connector

# Load credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")  # fallback default

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    role=SNOWFLAKE_ROLE
)

cursor = conn.cursor()

def get_schemas():
    cursor.execute(f"""
        SELECT SCHEMA_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
    """)
    return [row[0] for row in cursor.fetchall()]

def get_objects(schema, object_type):
    if object_type == "TABLES":
        query = f"""
            SELECT TABLE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE'
        """
    elif object_type == "VIEWS":
        query = f"""
            SELECT TABLE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = '{schema}'
        """
    elif object_type == "PROCEDURES":
        query = f"""
            SELECT PROCEDURE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.PROCEDURES
            WHERE PROCEDURE_SCHEMA = '{schema}'
        """
    else:
        raise ValueError(f"Unsupported object type: {object_type}")

    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]

def export_ddl(schema, object_type, name):
    folder = f"{object_type}s"  # Tables, Views, Procedures
    out_path = os.path.join("Snowflake", SNOWFLAKE_DATABASE, schema, folder)
    os.makedirs(out_path, exist_ok=True)

    # Add '()' to procedure names to match Snowflake's GET_DDL requirement
    full_name = f"{SNOWFLAKE_DATABASE}.{schema}.{name}"
    if object_type == "PROCEDURES":
        full_name += "()"

    try:
        cursor.execute(f"SELECT GET_DDL('{object_type}', '{full_name}')")
        ddl = cursor.fetchone()[0]
        with open(os.path.join(out_path, f"{name}.sql"), "w") as f:
            f.write(ddl)
        print(f"✅ Exported {object_type} {full_name}")
    except Exception as e:
        print(f"❌ Failed to export {object_type} {full_name}: {e}")

def main():
    for schema in get_schemas():
        print(f"\n🔍 Processing schema: {schema}")
        for obj_type in ["TABLES", "VIEWS", "PROCEDURES"]:
            try:
                objects = get_objects(schema, obj_type)
                print(f"📦 Found {len(objects)} {obj_type}(s) in {schema}")
                for obj in objects:
                    export_ddl(schema, obj_type, obj)
            except Exception as e:
                print(f"❌ Error processing {obj_type} in {schema}: {e}")

if __name__ == "__main__":
    main()
