from database.db import get_connection

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    with open("database/schema.sql", "r") as f:
        schema = f.read()

    cursor.executescript(schema)

    conn.commit()
    conn.close()

    print("✅ Database initialized successfully")

if __name__ == "__main__":
    init_db()