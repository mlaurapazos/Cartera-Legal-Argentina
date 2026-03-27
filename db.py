"""
db.py — Operaciones SQLite para Cartera Legal Analytics
"""
import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / "cartera.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_suscripciones (
            fecha TEXT, account_name TEXT, producto_principal_sf TEXT,
            producto TEXT, sold_to_pt TEXT, large_account TEXT,
            cant_usuarios TEXT, customer_class TEXT, customer_group TEXT,
            sector TEXT, subsector TEXT, industria_latam TEXT,
            sub_industria_latam TEXT, pais TEXT, ciudad TEXT,
            type_sf TEXT, tax_number TEXT, material TEXT,
            material_desc TEXT, acv_ars REAL, max_acv REAL,
            billing_value REAL, bu2 TEXT
        );

        CREATE TABLE IF NOT EXISTS clasificaciones (
            material TEXT PRIMARY KEY,
            es_principal INTEGER DEFAULT 0,
            producto_principal TEXT
        );

        CREATE TABLE IF NOT EXISTS resumen_mensual (
            periodo TEXT,
            sold_to_pt TEXT,
            account_name TEXT,
            producto_principal_sf TEXT,
            total_acv_ars REAL,
            valor_mensual_ars REAL,
            cant_tematicas INTEGER,
            cant_bibliotecas INTEGER,
            cant_revistas INTEGER,
            tiene_checkpoint INTEGER,
            producto_principal_suscripto TEXT,
            PRIMARY KEY (periodo, sold_to_pt)
        );

        CREATE TABLE IF NOT EXISTS upload_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fuente TEXT,
            periodo TEXT,
            fecha_carga TEXT,
            filas INTEGER
        );
        """)


def replace_raw(df: pd.DataFrame):
    with get_conn() as conn:
        df.to_sql("raw_suscripciones", conn, if_exists="replace", index=False)
        conn.commit()


def save_clasificaciones(df: pd.DataFrame):
    with get_conn() as conn:
        df.to_sql("clasificaciones", conn, if_exists="replace", index=False)
        conn.commit()


def get_clasificaciones() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql("SELECT * FROM clasificaciones ORDER BY material", conn)


def clasificaciones_vacio() -> bool:
    with get_conn() as conn:
        n = conn.execute("SELECT COUNT(*) FROM clasificaciones").fetchone()[0]
    return n == 0


def save_resumen_periodo(df: pd.DataFrame, periodo: str):
    """Reemplaza el período en resumen_mensual (upsert completo del período)."""
    with get_conn() as conn:
        conn.execute("DELETE FROM resumen_mensual WHERE periodo = ?", (periodo,))
        df["periodo"] = periodo
        df.to_sql("resumen_mensual", conn, if_exists="append", index=False)
        conn.commit()


def get_resumen(periodo: str = None) -> pd.DataFrame:
    with get_conn() as conn:
        if periodo:
            return pd.read_sql(
                "SELECT * FROM resumen_mensual WHERE periodo = ? ORDER BY total_acv_ars DESC",
                conn, params=(periodo,)
            )
        return pd.read_sql(
            "SELECT * FROM resumen_mensual ORDER BY periodo DESC, total_acv_ars DESC", conn
        )


def get_periodos() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT periodo FROM resumen_mensual ORDER BY periodo DESC"
        ).fetchall()
    return [r[0] for r in rows]


def log_upload(fuente: str, periodo: str, filas: int):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO upload_log (fuente, periodo, fecha_carga, filas) VALUES (?,?,datetime('now','localtime'),?)",
            (fuente, periodo, filas)
        )
        conn.commit()


def get_upload_log() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(
            "SELECT * FROM upload_log ORDER BY fecha_carga DESC LIMIT 50", conn
        )
