import os
import sqlite3


def migrate() -> None:
    db_path = "./app.db"
    if not os.path.exists(db_path):
        print("No existing database. Run 'python -m app.seed_admin' for a fresh setup.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(user)")
    cols = {row[1] for row in cur.fetchall()}

    if "rank" not in cols:
        cur.execute("ALTER TABLE user ADD COLUMN rank INTEGER NOT NULL DEFAULT 1")
        cur.execute("UPDATE user SET rank = 12 WHERE is_admin = 1")
        print(f"Added rank column. Migrated {cur.rowcount} admin(s) to rank 12.")
    else:
        print("rank column already exists.")

    conn.commit()
    conn.close()

    from app.models import create_db_and_tables
    create_db_and_tables()
    print("New tables created. Migration complete.")


if __name__ == "__main__":
    migrate()
