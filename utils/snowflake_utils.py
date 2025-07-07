import os
import snowflake.connector

def execute_sql_file(file_path):
    """Execute SQL file against Snowflake using credentials from environment variables."""

    # Read SQL file content
    with open(file_path, 'r') as f:
        sql_content = f.read()

    # Establish connection using environment variables
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

    try:
        cs = conn.cursor()
        for stmt in sql_content.split(';'):
            stmt = stmt.strip()
            if stmt:
                print(f"🛠 Executing: {stmt[:80]}...")
                cs.execute(stmt)
        cs.close()
        print("✅ SQL execution complete.")
    finally:
        conn.close()
