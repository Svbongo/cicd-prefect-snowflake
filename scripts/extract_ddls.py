import os
import snowflake.connector

# Load credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

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

# Object type → folder and Snowflake keyword mappings
OBJECT_MAP = {
    "TABLES": {"folder": "Tables", "type": "TABLE"},
    "VIEWS": {"folder": "Views", "type": "VIEW"},
    "PROCEDURES": {"folder": "Procedures", "type": "PROCEDURE"},
}

def get_schemas():
    cursor.execute(f"""
        SELECT SCHEMA_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
    """)
    return [row[0] for row in cursor.fetchall()]

def get_objects(schema, object_key):
    if object_key == "TABLES":
        cursor.execute(f"""
            SELECT TABLE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE'
        """)
    elif object_key == "VIEWS":
        cursor.execute(f"""
            SELECT TABLE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = '{schema}'
        """)
    elif object_key == "PROCEDURES":
        cursor.execute(f"""
            SELECT PROCEDURE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.PROCEDURES
            WHERE PROCEDURE_SCHEMA = '{schema}'
        """)
    else:
        raise ValueError(f"Unsupported object type: {object_key}")

    return [row[0] for row in cursor.fetchall()]

def export_ddl(schema, object_key, name):
    folder_name = OBJECT_MAP[object_key]["folder"]
    snowflake_type = OBJECT_MAP[object_key]["type"]

    out_path = os.path.join(
        "Snowflake",
        SNOWFLAKE_DATABASE.upper(),
        schema.upper(),
        folder_name
    )
    os.makedirs(out_path, exist_ok=True)

    file_path = os.path.join(out_path, f"{name.upper()}.sql")
    full_name = f"{SNOWFLAKE_DATABASE}.{schema}.{name}"
    if object_key == "PROCEDURES":
        full_name += "()"  # Required for procedures

    try:
        # Write DDL
        cursor.execute(f"SELECT GET_DDL('{snowflake_type}', '{full_name}')")
        ddl = cursor.fetchone()[0]

        with open(file_path, "w") as f:
            f.write(ddl + ";\n\n")

        print(f"✅ Exported {snowflake_type} {full_name}")

        # Write INSERTs for TABLES only
        if object_key == "TABLES":
            cursor.execute(f"SELECT * FROM {SNOWFLAKE_DATABASE}.{schema}.{name}")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            with open(file_path, "a") as f:
                for row in rows:
                    values = []
                    for val in row:
                        if val is None:
                            values.append('NULL')
                        elif isinstance(val, str):
                            escaped = val.replace("'", "''")
                            values.append("'{}'".format(escaped))
                        else:
                            values.append(str(val))
                    insert_stmt = f"INSERT INTO {name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
                    f.write(insert_stmt)
    except Exception as e:
        print(f"❌ Failed to export {snowflake_type} {full_name}: {e}")
def main():
    for schema in get_schemas():
        print(f"\n🔍 Processing schema: {schema}")
        for object_key in OBJECT_MAP.keys():
            try:
                objects = get_objects(schema, object_key)
                print(f"📦 Found {len(objects)} {object_key} in {schema}")
                for obj in objects:
                    export_ddl(schema, object_key, obj)
            except Exception as e:
                print(f"❌ Error processing {object_key} in {schema}: {e}")

if __name__ == "__main__":
    main()
