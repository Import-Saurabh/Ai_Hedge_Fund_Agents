import sqlite3

DB_PATH = "database/Ai_Hedge_Fund.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn