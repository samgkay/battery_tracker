import os
import sys
from glob import glob
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def get_database_url() -> str:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Create a .env file in the repo root.")
    return database_url


def read_migration_files() -> list[str]:
    base_dir = Path(__file__).resolve().parent.parent
    patterns = [
        base_dir / "sql" / "*.sql",
        base_dir / "sql" / "migrations" / "*.sql",
    ]
    files: list[str] = []
    for pattern in patterns:
        files.extend(glob(str(pattern)))
    return sorted(files)


def apply_migration_file(cursor, path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    if not sql.strip():
        print(f"Skipping empty migration file: {path}")
        return
    print(f"Applying migration: {path}")
    cursor.execute(sql)


def main() -> None:
    database_url = get_database_url()
    migration_files = read_migration_files()
    if not migration_files:
        print("No migration files found.")
        return

    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            for path in migration_files:
                apply_migration_file(cur, path)
        conn.commit()

    print("Migrations applied successfully.")


if __name__ == "__main__":
    # Ensure repo root is on sys.path for consistency
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root))
    main()
