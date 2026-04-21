"""
4_Suscripciones.py — Detalle de suscripciones activas con materiales nuevos
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from io import BytesIO

sys.path.insert(0, str(Path(__file__).parent.parent))
import db
import etl

st.set_page_config(page_title="Suscripciones · Cartera Legal", layout="wide")

st.markdown(
    "<h1 style='color:#1a3a5c;border-bottom:3px solid #c8102e;padding-bottom:8px'>"
    "📋 Detalle de Suscripciones Activas</h1>",
    unsafe_allow_html=True,
)

periodos = db.get_periodos()
if not periodos:
    st.warning("No hay períodos cargados. Cargá datos en **Carga de Datos**.")
    st.stop()

periodo = st.selectbox("Período", periodos, index=0)

with st.spinner("Calculando equivalencias WL..."):
    df = etl.build_detalle_suscripciones(periodo)

if df.empty:
    st.warning("No hay datos de suscripciones para este período.")
    st.stop()

# ── Filtro rápido por cliente ─────────────────────────────────────────────────
buscar = st.text_input("Buscar cliente (nombre o SAP)", "")
if buscar.strip():
    mask = (
        df["account_name"].astype(str).str.upper().str.contains(buscar.strip().upper(), na=False)
        | df["sold_to_pt"].astype(str).str.contains(buscar.strip(), na=False)
    )
    df = df[mask]

st.caption(f"{len(df):,} filas · {df['sold_to_pt'].nunique():,} clientes")

# ── Renombrar columnas para visualización ─────────────────────────────────────
display = df.rename(columns={
    "sold_to_pt":               "SAP ID",
    "account_name":             "Razón Social",
    "cant_usuarios":            "Usuarios",
    "mat_code_actual":          "Cód. Material",
    "mat_desc_actual":          "Descripción Material",
    "acv_anual_actual":         "ACV Anual",
    "acv_mensual_actual":       "ACV Mensual",
    "mat_code_nuevo":           "Cód. Material Nuevo",
    "mat_desc_nuevo":           "Descripción Material Nuevo",
    "acv_anual_nuevo":          "ACV Anual Nuevo",
    "acv_mensual_nuevo":        "ACV Mensual Nuevo",
    "suscripcion_mensual_nueva": "Suscripción Mensual Nueva",
})

# ── Grilla ────────────────────────────────────────────────────────────────────
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

    gb = GridOptionsBuilder.from_dataframe(display)
    gb.configure_default_column(
        sortable=True, filter=True, resizable=True,
        wrapText=False, autoHeight=False,
    )
    for col in ["ACV Anual", "ACV Mensual", "ACV Anual Nuevo", "ACV Mensual Nuevo", "Suscripción Mensual Nueva"]:
        gb.configure_column(
            col,
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter="'$ ' + value.toLocaleString('es-AR', {minimumFractionDigits:2, maximumFractionDigits:2})",
        )
    gb.configure_column("Usuarios", type=["numericColumn"])
    gb.configure_column("SAP ID", pinned="left", width=120)
    gb.configure_column("Razón Social", pinned="left", width=220)

    AgGrid(
        display,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.NO_UPDATE,
        height=600,
        use_container_width=True,
        allow_unsafe_jscode=True,
    )
except ImportError:
    st.dataframe(
        display.style.format({
            "ACV Anual":                "$ {:,.2f}",
            "ACV Mensual":              "$ {:,.2f}",
            "ACV Anual Nuevo":          "$ {:,.2f}",
            "ACV Mensual Nuevo":        "$ {:,.2f}",
            "Suscripción Mensual Nueva": "$ {:,.2f}",
        }),
        use_container_width=True,
        height=600,
    )

# ── Descarga ──────────────────────────────────────────────────────────────────
buf = BytesIO()
display.to_excel(buf, index=False, engine="openpyxl")
buf.seek(0)
st.download_button(
    label="⬇ Descargar Excel",
    data=buf,
    file_name=f"suscripciones_{periodo}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
