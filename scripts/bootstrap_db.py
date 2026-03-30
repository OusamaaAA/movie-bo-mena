from pathlib import Path

from sqlalchemy import text

from src.db import engine


def apply_sql_file(path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    migrations = sorted((root / "migrations").glob("*.sql"))
    for file in migrations:
        apply_sql_file(file)
        print(f"Applied {file.name}")


if __name__ == "__main__":
    main()

