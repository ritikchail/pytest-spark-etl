sql_str = "CREATE SCHEMA IF NOT EXISTS rainforest;"

import psycopg2

# Connect to PostgreSQL database (Neon)
conn = psycopg2.connect(
    dbname="upstreamdb",
    user="neondb_owner",
    password="npg_TiHI0mtYE8au",
    host="ep-long-rain-am3oyzm3-pooler.c-5.us-east-1.aws.neon.tech",
    port="5432",
    sslmode="require",
)
cur = conn.cursor()

# Check connection
try:
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print("Connection successful! PostgreSQL version:", version[0])
except Exception as e:
    print("Connection failed:", str(e))
    conn.close()
    exit(1)

# Execute the SQL string
# try:
#     cur.execute(sql_str)
#     conn.commit()
#     print("SQL executed successfully:", sql_str)
# except Exception as e:
#     print("SQL execution failed:", str(e))
#     conn.rollback()

# Close connection
cur.close()
conn.close()