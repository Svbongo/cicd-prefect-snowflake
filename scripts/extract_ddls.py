import snowflake.connector
import os
import subprocess

# Configs from environment variables (GitHub secrets or .env)
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

output_base = './Snowflake/' + SNOWFLAKE_DATABASE

object_types = {
    'TABLE': 'Tables',
    'VIEW': 'Views',
    'PROCEDURE': 'Procedures'
}

def git_push(commit_message="Auto-sync: Snowflake DDLs"):
    try:
        repo_url = os.getenv("GITHUB_REPOSITORY")
        token = os.getenv("HUB_TOKEN")

        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)

        remote_url = f"https://x-access-token:{token}@github.com/{repo_url}.git"
        subprocess.run(["git", "remote", "set-url", "origin", remote_url], check=True)

        subprocess.run(["git", "push", "origin", "main"], check=True)

        print("✅ Changes pushed to main branch.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git operation failed: {e}")


def extract_ddls():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE
    )

    cursor = conn.cursor()

    cursor.execute(f"SHOW SCHEMAS IN DATABASE {SNOWFLAKE_DATABASE}")
    # Exclude system schemas
    schemas = [row[1] for row in cursor.fetchall() if row[1] not in ['INFORMATION_SCHEMA']]

    for schema in schemas:
        schema_base = os.path.join(output_base, schema)

        for folder in object_types.values():
            os.makedirs(os.path.join(schema_base, folder), exist_ok=True)

        print(f"🔍 Processing schema: {schema}")

        for obj_type, folder in object_types.items():
            try:
                show_cmd = f"SHOW {obj_type}s IN SCHEMA {SNOWFLAKE_DATABASE}.{schema}"
                cursor.execute(show_cmd)
                objects = cursor.fetchall()

                for obj in objects:
                    name = obj[1]
                    ddl_cursor = conn.cursor()
                    ddl_cursor.execute(f"SELECT GET_DDL('{obj_type}', '{SNOWFLAKE_DATABASE}.{schema}.{name}')")
                    ddl = ddl_cursor.fetchone()[0]

                    file_path = os.path.join(schema_base, folder, f"{name}.sql")
                    with open(file_path, 'w') as f:
                        f.write(ddl + ';\n')

                    ddl_cursor.close()

            except Exception as e:
                print(f"⚠️ Skipped {obj_type}s for {schema}: {e}")

    cursor.close()
    conn.close()

    print("✅ DDL extraction complete.")
    git_push()

if __name__ == "__main__":
    extract_ddls()
