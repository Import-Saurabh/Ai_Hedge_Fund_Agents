import os
from database.db import get_connection

def init_db():
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, encoding='utf-8') as f:
        sql = f.read()
    conn = get_connection()
    conn.executescript(sql)
    conn.commit()
    conn.close()
    print("✅ DB initialised (all tables, v3)")

# Add these two lines at the very end!
if __name__ == "__main__":
    init_db()