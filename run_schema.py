import os
import psycopg2
from pathlib import Path

# === Config ===
SQL_PATH = Path("/Users/ritikchail/Documents/personal/spark_workspace/pytest-zero2hero/project2/create_tables_schema.sql")

DB_NAME = os.getenv("NEON_DBNAME", "upstreamdb")
DB_USER = os.getenv("NEON_USER", "neondb_owner")
DB_PASSWORD = os.getenv("NEON_PASSWORD", "npg_TiHI0mtYE8au")
DB_HOST = os.getenv("NEON_HOST", "ep-long-rain-am3oyzm3-pooler.c-5.us-east-1.aws.neon.tech")
DB_PORT = os.getenv("NEON_PORT", "5432")
DB_SSLMODE = os.getenv("NEON_SSLMODE", "require")

if not SQL_PATH.exists():
    raise FileNotFoundError(f"SQL file not found: {SQL_PATH}")

sql = SQL_PATH.read_text()


conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    sslmode=DB_SSLMODE,
)

try:
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with conn, conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    print("✅ Schema/tables created successfully.")
except Exception as ex:
    print("❌ Failed to run schema script:", ex)
    raise
finally:
    conn.close()
