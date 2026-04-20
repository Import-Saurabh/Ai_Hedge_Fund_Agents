"""
database/init_db.py  v4.0
Reads schema.sql from same directory and creates all tables.
"""
import os
from database.db import get_connection

def init_db():
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        sql = f.read()
    conn = get_connection()
    conn.executescript(sql)
    conn.commit()
    conn.close()
    print("  ok  DB initialised (schema v3)")