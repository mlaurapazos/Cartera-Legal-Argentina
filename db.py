"""
db.py — Operaciones PostgreSQL para Cartera Legal Analytics
"""
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from urllib.parse import urlparse


@st.cache_resource
def get_engine():
    raw = st.secrets["DATABASE_URL"]
    parsed = urlparse(raw)
    # Build URL properly so special characters in password are handled
    url = URL.create(
        drivername="postgresql+psycopg",
        username=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path.lstrip("/"),
        query={"sslmode": "require"},
    )
    return create_engine(url)


def get_conn():
    """Retorna el engine SQLAlchemy (compatible con pd.read_sql y etl.py)."""
    return get_engine()


def init_db():
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_suscripciones (
                fecha TEXT, account_name TEXT, producto_principal_sf TEXT,
                producto TEXT, sold_to_pt TEXT, large_account TEXT,
                cant_usuarios TEXT, customer_class TEXT, customer_group TEXT,
                sector TEXT, subsector TEXT, industria_latam TEXT,
                sub_industria_latam TEXT, pais TEXT, ciudad TEXT,
                type_sf TEXT, tax_number TEXT, material TEXT,
                material_desc TEXT, acv_ars REAL, max_acv REAL,
                billing_value REAL, bu2 TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS clasificaciones (
                material TEXT PRIMARY KEY,
                es_principal INTEGER DEFAULT 0,
                producto_principal TEXT
            )
        """))
        conn.execute(text("""
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
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS upload_log (
                id SERIAL PRIMARY KEY,
                fuente TEXT,
                periodo TEXT,
                fecha_carga TEXT,
                filas INTEGER
            )
        """))
        conn.commit()


def replace_raw(df: pd.DataFrame):
    engine = get_engine()
    df.to_sql("raw_suscripciones", engine, if_exists="replace", index=False)


def save_clasificaciones(df: pd.DataFrame):
    engine = get_engine()
    df.to_sql("clasificaciones", engine, if_exists="replace", index=False)


def get_clasificaciones() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM clasificaciones ORDER BY material",
        get_engine()
    )


def clasificaciones_vacio() -> bool:
    with get_engine().connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM clasificaciones"))
        return result.scalar() == 0


def save_resumen_periodo(df: pd.DataFrame, periodo: str):
    """Reemplaza el período en resumen_mensual (upsert completo del período)."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM resumen_mensual WHERE periodo = :periodo"),
            {"periodo": periodo}
        )
        conn.commit()
    df["periodo"] = periodo
    df.to_sql("resumen_mensual", engine, if_exists="append", index=False)


def get_resumen(periodo: str = None) -> pd.DataFrame:
    if periodo:
        return pd.read_sql(
            "SELECT * FROM resumen_mensual WHERE periodo = %(periodo)s ORDER BY total_acv_ars DESC",
            get_engine(), params={"periodo": periodo}
        )
    return pd.read_sql(
        "SELECT * FROM resumen_mensual ORDER BY periodo DESC, total_acv_ars DESC",
        get_engine()
    )


def get_periodos() -> list:
    with get_engine().connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT periodo FROM resumen_mensual ORDER BY periodo DESC")
        )
        return [r[0] for r in result.fetchall()]


def log_upload(fuente: str, periodo: str, filas: int):
    with get_engine().connect() as conn:
        conn.execute(
            text("INSERT INTO upload_log (fuente, periodo, fecha_carga, filas) VALUES (:fuente, :periodo, NOW()::text, :filas)"),
            {"fuente": fuente, "periodo": periodo, "filas": filas}
        )
        conn.commit()


def get_upload_log() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM upload_log ORDER BY fecha_carga DESC LIMIT 50",
        get_engine()
    )
