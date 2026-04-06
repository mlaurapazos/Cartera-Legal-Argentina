"""
db.py — Supabase (HTTPS) para Cartera Legal Analytics
"""
import math
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


def _clean(v):
    """Convierte NaN/inf a None para serialización JSON."""
    if v is None:
        return None
    try:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
    except Exception:
        pass
    return v


def _to_records(df: pd.DataFrame) -> list:
    """Convierte DataFrame a lista de dicts reemplazando NaN/inf con None."""
    return [{k: _clean(v) for k, v in row.items()} for row in df.to_dict(orient="records")]


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


def save_estructura(df: pd.DataFrame):
    client = get_client()
    client.table("estructura").delete().not_.is_("material", "null").execute()
    records = _to_records(df)
    for i in range(0, len(records), 500):
        client.table("estructura").insert(records[i:i + 500]).execute()


def get_estructura() -> pd.DataFrame:
    try:
        client = get_client()
        result = client.table("estructura").select("*").execute()
        return pd.DataFrame(result.data) if result.data else pd.DataFrame(
            columns=["material", "descripcion", "formato", "tem_gen", "produc", "lln_sil"]
        )
    except Exception:
        return pd.DataFrame(columns=["material", "descripcion", "formato", "tem_gen", "produc", "lln_sil"])


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


def _fetch_all(query_fn) -> list:
    """Pagina una consulta de Supabase hasta traer todas las filas."""
    all_data, page_size, offset = [], 1000, 0
    while True:
        result = query_fn(offset, offset + page_size - 1)
        all_data.extend(result.data or [])
        if len(result.data or []) < page_size:
            break
        offset += page_size
    return all_data


def get_resumen(periodo: str = None) -> pd.DataFrame:
    client = get_client()
    if periodo:
        data = _fetch_all(lambda s, e: client.table("resumen_mensual")
                          .select("*").eq("periodo", periodo)
                          .order("total_acv_ars", desc=True).range(s, e).execute())
    else:
        data = _fetch_all(lambda s, e: client.table("resumen_mensual")
                          .select("*").order("total_acv_ars", desc=True).range(s, e).execute())
    df = pd.DataFrame(data) if data else pd.DataFrame()
    if not df.empty and periodo:
        uso = get_uso(periodo)
        if not uso.empty:
            df = df.merge(uso, on="sold_to_pt", how="left")
            df["uso_sil"] = df["uso_sil"].fillna(0).astype(int)
            df["uso_lln"] = df["uso_lln"].fillna(0).astype(int)
        aging = get_aging(periodo)
        if not aging.empty:
            df = df.merge(aging, on="sold_to_pt", how="left")
            for col in ["deuda_90", "deuda_180", "deuda_360"]:
                df[col] = df[col].fillna(0)
    return df


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


def save_equiv_wl(df: pd.DataFrame):
    client = get_client()
    client.table("equiv_wl").delete().not_.is_("mat_actual", "null").execute()
    records = _to_records(df)
    for i in range(0, len(records), 500):
        client.table("equiv_wl").insert(records[i:i + 500]).execute()


def get_equiv_wl() -> pd.DataFrame:
    try:
        client = get_client()
        result = client.table("equiv_wl").select("*").execute()
        return pd.DataFrame(result.data) if result.data else pd.DataFrame(
            columns=["mat_actual", "mat_nuevo_1", "mat_nuevo_2", "mat_nuevo_3"]
        )
    except Exception:
        return pd.DataFrame(columns=["mat_actual", "mat_nuevo_1", "mat_nuevo_2", "mat_nuevo_3"])


def save_precios_wl(df: pd.DataFrame):
    client = get_client()
    client.table("precios_wl").delete().not_.is_("material", "null").execute()
    records = _to_records(df)
    for i in range(0, len(records), 500):
        client.table("precios_wl").insert(records[i:i + 500]).execute()


def get_precios_wl() -> pd.DataFrame:
    try:
        client = get_client()
        result = client.table("precios_wl").select("*").execute()
        return pd.DataFrame(result.data) if result.data else pd.DataFrame(
            columns=["material", "acv_anual", "acv_mensual"]
        )
    except Exception:
        return pd.DataFrame(columns=["material", "acv_anual", "acv_mensual"])


def save_uso_periodo(df: pd.DataFrame, periodo: str):
    client = get_client()
    client.table("uso_mensual").delete().eq("periodo", periodo).execute()
    df = df.copy()
    df["periodo"] = periodo
    records = _to_records(df)
    for i in range(0, len(records), 500):
        client.table("uso_mensual").insert(records[i:i + 500]).execute()


def get_uso(periodo: str) -> pd.DataFrame:
    try:
        client = get_client()
        result = (client.table("uso_mensual")
                  .select("sold_to_pt,uso_sil,uso_lln")
                  .eq("periodo", periodo)
                  .execute())
        return pd.DataFrame(result.data) if result.data else pd.DataFrame(
            columns=["sold_to_pt", "uso_sil", "uso_lln"]
        )
    except Exception:
        return pd.DataFrame(columns=["sold_to_pt", "uso_sil", "uso_lln"])


def save_aging_periodo(df: pd.DataFrame, periodo: str):
    """Guarda datos de aging (deuda) para un período. Tabla: aging_mensual."""
    client = get_client()
    client.table("aging_mensual").delete().eq("periodo", periodo).execute()
    df = df.copy()
    df["periodo"] = periodo
    records = _to_records(df)
    for i in range(0, len(records), 500):
        client.table("aging_mensual").insert(records[i:i + 500]).execute()


def get_aging(periodo: str) -> pd.DataFrame:
    try:
        client = get_client()
        result = (client.table("aging_mensual")
                  .select("sold_to_pt,deuda_90,deuda_180,deuda_360")
                  .eq("periodo", periodo)
                  .execute())
        return pd.DataFrame(result.data) if result.data else pd.DataFrame(
            columns=["sold_to_pt", "deuda_90", "deuda_180", "deuda_360"]
        )
    except Exception:
        return pd.DataFrame(columns=["sold_to_pt", "deuda_90", "deuda_180", "deuda_360"])


def get_upload_log() -> pd.DataFrame:
    client = get_client()
    result = (client.table("upload_log")
              .select("*")
              .order("fecha_carga", desc=True)
              .limit(50)
              .execute())
    return pd.DataFrame(result.data) if result.data else pd.DataFrame()
