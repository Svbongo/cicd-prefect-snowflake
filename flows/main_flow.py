from prefect import flow, task
import snowflake.connector
import os

@task
def run_sql_file(file_path):
    print(f"Running SQL file: {file_path}")
    with open(file_path, 'r') as f:
        sql = f.read()

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="COMPUTE_WH",
        database="DEMO_DB",
        schema="DATA_PIPELINE"
    )
    cursor = conn.cursor()
    for stmt in sql.split(";"):
        if stmt.strip():
            cursor.execute(stmt)
    cursor.close()
    conn.close()

@flow(name="Snowflake_CICD_Flow")
def main_flow():
    run_sql_file("sql/create_schema.sql")
    run_sql_file("sql/test_data.sql")

if __name__ == "__main__":
    main_flow()
