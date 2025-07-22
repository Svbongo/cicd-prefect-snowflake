import snowflake.connector
import os
import subprocess

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
HUB_TOKEN = os.getenv("HUB_TOKEN")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY")

output_base = './Snowflake/' + SNOWFLAKE_DATABASE

object_types = {
    'TABLE': 'Tables',
    'VIEW': 'Views',
    'PROCEDURE': 'Procedures'
}

def export_table(cursor, database, schema, table, output_path):
    # Get CREATE TABLE DDL
    cursor.execute(f"SELECT GET_DDL('TABLE', '{database}.{schema}.{table}')")
    ddl = cursor.fetchone()[0]

    # Get INSERT statements
    cursor.execute(f"SELECT * FROM {database}.{schema}.{table}")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    insert_statements = []
    for row in rows:
        values = []
        for val in row:
            if val is None:
                values.append("NULL")
            elif isinstance(val, str):
                values.append(f"'{val.replace('\'', '\'\'')}'")  # Escape single quotes
            else:
                values.append(str(val))
        insert_stmt = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)});"
        insert_statements.append(insert_stmt)

    # Write combined file
    with open(output_path, 'w') as f:
        f.write(ddl + ';\n\n')
        for stmt in insert_statements:
            f.write(stmt + '\n')

def export_object(cursor, object_type, database, schema, name, output_path):
    cursor.execute(f"SELECT GET_DDL('{object_type}', '{database}.{schema}.{name}')")
    ddl = cursor.fetchone()[0]
    with open(output_path, 'w') as f:
        f.write(ddl + ';\n')

def git_push(commit_message="Auto-sync: Snowflake DDLs and Data"):
    try:
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)

        subprocess.run(["git", "remote", "remove", "origin"], check=True)
        remote_url = f"https://x-access-token:{HUB_TOKEN}@github.com/{GITHUB_REPOSITORY}.git"
        subprocess.run(["git", "remote", "add", "origin", remote_url], check=True)

        subprocess.run(["git", "config", "--local", "--unset-all", "http.https://github.com/.extraheader"], check=True)

        subprocess.run(["git", "push", "origin", "main"], check=True)

        print("✅ Changes pushed to main branch.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git operation failed: {e}")

def extract_all():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE
    )

    cursor = conn.cursor()

    cursor.execute(f"SHOW SCHEMAS IN DATABASE {SNOWFLAKE_DATABASE}")
    schemas = [row[1] for row in cursor.fetchall() if row[1] not in ['INFORMATION_SCHEMA']]

    for schema in schemas:
        print(f"🔍 Processing schema: {schema}")

        schema_base = os.path.join(output_base, schema)
        for folder in object_types.values():
            os.makedirs(os.path.join(schema_base, folder), exist_ok=True)

        for obj_type, folder in object_types.items():
            try:
                show_cmd = f"SHOW {obj_type}s IN SCHEMA {SNOWFLAKE_DATABASE}.{schema}"
                cursor.execute(show_cmd)
                objects = cursor.fetchall()

                for obj in objects:
                    name = obj[1]
                    output_path = os.path.join(schema_base, folder, f"{name}.sql")

                    if obj_type == 'TABLE':
                        export_table(cursor, SNOWFLAKE_DATABASE, schema, name, output_path)
                    else:
                        export_object(cursor, obj_type, SNOWFLAKE_DATABASE, schema, name, output_path)

            except Exception as e:
                print(f"⚠️ Skipped {obj_type}s for {schema}: {e}")

    cursor.close()
    conn.close()

    print("✅ DDL + Data extraction complete.")
    git_push()

if __name__ == "__main__":
    extract_all()
