from prefect import flow, task
import os

@task
def read_sql_file_list(file_path):
    print(f"📖 Reading SQL file list from: {file_path}")
    with open(file_path, 'r') as file:
        sql_files = [line.strip() for line in file if line.strip()]
    print(f"📂 SQL Files from {file_path}:")
    for sql in sql_files:
        print(sql)
    return sql_files

@task
def categorize_sql_files(sql_files):
    categories = {
        "TABLES": [],
        "VIEWS": [],
        "PROCEDURES": [],
        "TRIGGERS": []
    }

    for path in sql_files:
        path_upper = path.upper()
        parts = path_upper.split('/')
        matched = False
        for part in parts:
            part = part.strip()
            if part in categories:
                categories[part].append(path)
                matched = True
                break
        if not matched:
            print(f"⚠️ Unrecognized path (skipped): {path}")

    for category, files in categories.items():
        print(f"\n📂 Category: {category}")
        if not files:
            print("⚠️ No files found")
        else:
            for f in files:
                print(f"  └─ {f}")

    return categories


@flow(name="main-flow")
def main_flow(release_notes: str = "sorted_sql.txt"):
    sql_files = read_sql_file_list(release_notes)
    categorized_files = categorize_sql_files(sql_files)
    # You can now pass `categorized_files` into your execution logic
    return categorized_files

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("❌ Usage: python main_flow.py --release-notes <path_to_sorted_sql.txt>")
        sys.exit(1)

    release_notes_path = sys.argv[2] if sys.argv[1] == "--release-notes" else None
    if not release_notes_path or not os.path.exists(release_notes_path):
        print(f"❌ Invalid or missing release notes file: {release_notes_path}")
        sys.exit(1)

    main_flow(release_notes_path)
