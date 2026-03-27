"""
db.py — Supabase (HTTPS) para Cartera Legal Analytics
"""
import pandas as pd
import streamlit as st
from supabase import create_client, Client


@st.cache_resource
def get_client() -> Client:
    return create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_KEY"],
    )


def get_conn():
    """Compatibilidad con llamadas existentes — no se usa."""
    return None


def _to_records(df: pd.DataFrame) -> list:
    """Convierte DataFrame a lista de dicts reemplazando NaN/inf con None."""
    return (df
            .replace([float("inf"), float("-inf")], None)
            .where(pd.notna(df), None)
            .to_dict(orient="records"))


def init_db():
    """Las tablas se crean desde el SQL Editor de Supabase (setup inicial)."""
    pass


def replace_raw(df: pd.DataFrame):
    client = get_client()
    client.table("raw_suscripciones").delete().not_.is_("sold_to_pt", "null").execute()
    records = _to_records(df)
    for i in range(0, len(records), 500):
        client.table("raw_suscripciones").insert(records[i:i + 500]).execute()


def save_clasificaciones(df: pd.DataFrame):
    client = get_client()
    client.table("clasificaciones").delete().not_.is_("material", "null").execute()
    records = _to_records(df)
    for i in range(0, len(records), 500):
        client.table("clasificaciones").insert(records[i:i + 500]).execute()


def get_clasificaciones() -> pd.DataFrame:
    client = get_client()
    result = client.table("clasificaciones").select("*").order("material").execute()
    return pd.DataFrame(result.data) if result.data else pd.DataFrame(
        columns=["material", "es_principal", "producto_principal"]
    )


def clasificaciones_vacio() -> bool:
    client = get_client()
    result = client.table("clasificaciones").select("material", count="exact").limit(1).execute()
    return (result.count or 0) == 0


def get_raw_suscripciones() -> pd.DataFrame:
    client = get_client()
    all_data = []
    page_size = 1000
    offset = 0
    while True:
        result = (client.table("raw_suscripciones")
                  .select("*")
                  .range(offset, offset + page_size - 1)
                  .execute())
        all_data.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size
    return pd.DataFrame(all_data)


def save_resumen_periodo(df: pd.DataFrame, periodo: str):
    client = get_client()
    client.table("resumen_mensual").delete().eq("periodo", periodo).execute()
    df["periodo"] = periodo
    records = _to_records(df)
    for i in range(0, len(records), 500):
        client.table("resumen_mensual").insert(records[i:i + 500]).execute()


def get_resumen(periodo: str = None) -> pd.DataFrame:
    client = get_client()
    if periodo:
        result = (client.table("resumen_mensual")
                  .select("*")
                  .eq("periodo", periodo)
                  .order("total_acv_ars", desc=True)
                  .execute())
    else:
        result = (client.table("resumen_mensual")
                  .select("*")
                  .order("total_acv_ars", desc=True)
                  .execute())
    return pd.DataFrame(result.data) if result.data else pd.DataFrame()


def get_periodos() -> list:
    client = get_client()
    result = (client.table("resumen_mensual")
              .select("periodo")
              .order("periodo", desc=True)
              .execute())
    seen, periodos = set(), []
    for r in (result.data or []):
        p = r["periodo"]
        if p not in seen:
            seen.add(p)
            periodos.append(p)
    return periodos


def log_upload(fuente: str, periodo: str, filas: int):
    client = get_client()
    client.table("upload_log").insert({
        "fuente": fuente,
        "periodo": periodo,
        "fecha_carga": pd.Timestamp.now().isoformat(),
        "filas": filas,
    }).execute()


def get_upload_log() -> pd.DataFrame:
    client = get_client()
    result = (client.table("upload_log")
              .select("*")
              .order("fecha_carga", desc=True)
              .limit(50)
              .execute())
    return pd.DataFrame(result.data) if result.data else pd.DataFrame()
