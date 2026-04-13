from database.db import get_connection


def log_run(symbol: str, modules_ok: list, modules_warn: list, version: str = "v5"):
    conn = get_connection()
    conn.execute("""
        INSERT INTO run_log (symbol, script_version, modules_ok, modules_warn)
        VALUES (?, ?, ?, ?)
    """, (
        symbol, version,
        ",".join(modules_ok),
        ",".join(modules_warn),
    ))
    conn.commit()
    conn.close()
