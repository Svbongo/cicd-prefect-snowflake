import os
import snowflake.connector

# Load credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")  # default fallback

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
    if object_type in ["TABLE", "VIEW"]:
        info_schema = "TABLES" if object_type == "TABLE" else "VIEWS"
        name_col = "TABLE_NAME"
        schema_col = "TABLE_SCHEMA"
    elif object_type == "PROCEDURE":
        info_schema = "PROCEDURES"
        name_col = "PROCEDURE_NAME"
        schema_col = "PROCEDURE_SCHEMA"
    else:
        raise ValueError(f"Unsupported object type: {object_type}")

    query = f"""
        SELECT {name_col} FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.{info_schema}
        WHERE {schema_col} = '{schema}'
    """
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]


def export_ddl(schema, object_type, name):
    ddl_path = os.path.join("Snowflake", SNOWFLAKE_DATABASE, schema, f"{object_type}s", f"{name}.sql")
    os.makedirs(os.path.dirname(ddl_path), exist_ok=True)
    
    try:
        qualified_name = f"{SNOWFLAKE_DATABASE}.{schema}.{name}"
        cursor.execute(f"SELECT GET_DDL('{object_type}', '{qualified_name}')")
        ddl = cursor.fetchone()[0]
        with open(ddl_path, "w") as f:
            f.write(ddl)
        print(f"✅ Exported {object_type} {qualified_name}")
    except Exception as e:
        print(f"❌ Failed to export {object_type} {qualified_name}: {e}")

def main():
    for schema in get_schemas():
        print(f"\n🔍 Processing schema: {schema}")
        for obj_type in ["TABLE", "VIEW", "PROCEDURE"]:
            objects = get_objects(schema, obj_type)
            print(f"📦 Found {len(objects)} {obj_type}(s) in {schema}")
            for obj in objects:
                export_ddl(schema, obj_type, obj)

if __name__ == "__main__":
    main()
