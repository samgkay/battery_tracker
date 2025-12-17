import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def load_database_url() -> str:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Create a .env file with DATABASE_URL.")
    return database_url


def apply_migration(cur, migration_path: Path) -> None:
    with migration_path.open("r", encoding="utf-8") as f:
        sql = f.read()
    cur.execute(sql)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    migrations_dir = repo_root / "sql" / "migrations"
    if not migrations_dir.exists():
        print(f"No migrations directory found at {migrations_dir}")
        sys.exit(1)

    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        print(f"No migration files found in {migrations_dir}")
        sys.exit(0)

    database_url = load_database_url()

    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            for migration_file in migration_files:
                print(f"Applying migration: {migration_file.name}")
                apply_migration(cur, migration_file)
        conn.commit()

    print("Migrations applied successfully.")


if __name__ == "__main__":
    main()
