"""
1_Carga_de_Datos.py — Carga mensual de suscripciones
"""
import re
import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import db
import etl

st.set_page_config(page_title="Carga de Datos · Cartera Legal", layout="wide")

st.markdown(
    "<h1 style='color:#1a3a5c;border-bottom:3px solid #c8102e;padding-bottom:8px'>"
    "📤 Carga de Datos</h1>",
    unsafe_allow_html=True,
)

# ── Sección 1: Suscripciones mensuales ────────────────────────────────────────
st.subheader("Suscripciones activas — carga mensual")

archivo = st.file_uploader(
    "Seleccioná el Excel mensual de suscripciones activas",
    type=["xlsx", "xls"],
    key="uploader_sus",
)

# Detectar período desde el nombre del archivo
periodo_default = ""
if archivo:
    m = re.search(r"(\d{2})(\d{4})", archivo.name)  # MMYYYY
    if m:
        periodo_default = f"{m.group(2)}-{m.group(1)}"
    else:
        m2 = re.search(r"(\d{4})-?(\d{2})", archivo.name)  # YYYY-MM o YYYYMM
        if m2:
            periodo_default = f"{m2.group(1)}-{m2.group(2)}"

periodo = st.text_input(
    "Período (YYYY-MM)",
    value=periodo_default,
    placeholder="2026-02",
    help="Identificador del período que estás cargando. Si ya existe se reemplaza.",
)

col_btn, col_info = st.columns([1, 3])
with col_btn:
    procesar = st.button("⚡ Procesar", type="primary", disabled=(archivo is None or not periodo))

if procesar and archivo and periodo:
    with st.spinner("Procesando suscripciones..."):
        try:
            file_bytes = archivo.read()
            df_raw = etl.load_suscripciones(file_bytes)
            db.replace_raw(df_raw)

            n = etl.build_resumen(db.get_conn(), periodo)
            db.log_upload("suscripciones", periodo, n)

            st.success(f"✅ {n:,} clientes procesados para el período **{periodo}**")
            st.toast(f"Período {periodo} cargado correctamente", icon="✅")
        except Exception as e:
            st.error(f"Error al procesar: {e}")

# ── Sección 2: Seed de Clasificaciones ────────────────────────────────────────
st.divider()
st.subheader("Clasificaciones de materiales")

n_clases = len(db.get_clasificaciones())
if n_clases > 0:
    st.info(f"Ya hay **{n_clases} materiales** clasificados en la base de datos. "
            f"Podés editarlos en la página **Clasificaciones**.")

with st.expander("🔄 Cargar clasificaciones desde Excel (reemplaza las existentes)"):
    st.warning("Esto reemplazará todas las clasificaciones actuales.")
    archivo_cl = st.file_uploader(
        "Seleccioná el Excel que contiene la hoja 'Clasificaciones'",
        type=["xlsx", "xls"],
        key="uploader_cl",
    )
    if st.button("Seed Clasificaciones", disabled=archivo_cl is None):
        with st.spinner("Cargando clasificaciones..."):
            try:
                n_cl = etl.seed_clasificaciones(db.get_conn(), archivo_cl.read())
                st.success(f"✅ {n_cl} materiales clasificados cargados.")

                # Recalcular todos los períodos existentes
                periodos = db.get_periodos()
                if periodos:
                    with st.spinner(f"Recalculando {len(periodos)} período(s)..."):
                        conn = db.get_conn()
                        for p in periodos:
                            etl.build_resumen(conn, p)
                    st.info(f"Resumen recalculado para {len(periodos)} período(s).")
            except Exception as e:
                st.error(f"Error: {e}")

# ── Historial de cargas ────────────────────────────────────────────────────────
st.divider()
st.subheader("Historial de cargas")

log = db.get_upload_log()
if log.empty:
    st.caption("Sin cargas registradas.")
else:
    st.dataframe(log, use_container_width=True, hide_index=True)
