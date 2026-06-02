from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "warehouse" / "citibike.duckdb"
SQL_DIR = BASE_DIR / "sql"

SQL_FILES = [
    "raw.sql",
    "staging.sql",
]

def run_sql_file(con, file_path: Path) -> None:
    sql = file_path.read_text(encoding="utf-8")
    con.execute(sql)

def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    try:
        for sql_file in SQL_FILES:
            path = SQL_DIR / sql_file
            print(f"Running {path.name}...")
            run_sql_file(con, path)

        print(f"Warehouse build complete: {DB_PATH}")
    finally:
        con.close()

if __name__ == "__main__":
    main()