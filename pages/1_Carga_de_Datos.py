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
            db.replace_raw_periodo(df_raw, periodo)

            n = etl.build_resumen(db.get_conn(), periodo)
            db.log_upload("suscripciones", periodo, n)

            st.success(f"✅ {n:,} clientes procesados para el período **{periodo}**")
            st.toast(f"Período {periodo} cargado correctamente", icon="✅")
        except Exception as e:
            st.error(f"Error al procesar: {e}")

# ── Sección 2: Seed de Estructura de planes ───────────────────────────────────
st.divider()
st.subheader("Estructura de planes — seed")
st.caption(
    "Archivo **V.2 2023 Estructura Planes SIL.-LLNEXT.xlsx**, hoja **LISTADO GRAL (2)**. "
    "Define Temáticas, Bibliotecas, Revistas y Producto Principal por material."
)

n_est = len(db.get_estructura())
if n_est > 0:
    st.info(f"Ya hay **{n_est} materiales** en la estructura de planes.")

with st.expander("🔄 Cargar estructura desde Excel (reemplaza la existente)"):
    st.warning("Esto reemplazará la estructura actual y recalculará todos los períodos.")
    archivo_est = st.file_uploader(
        "Seleccioná el Excel de estructura de planes",
        type=["xlsx"],
        key="uploader_est",
    )
    if st.button("Seed Estructura", disabled=archivo_est is None):
        with st.spinner("Cargando estructura..."):
            try:
                n_est_new = etl.seed_estructura(db.get_conn(), archivo_est.read())
                st.success(f"✅ {n_est_new} materiales cargados en la estructura.")
                periodos = db.get_periodos()
                if periodos:
                    with st.spinner(f"Recalculando {len(periodos)} período(s)..."):
                        for p in periodos:
                            etl.build_resumen(db.get_conn(), p)
                    st.info(f"Resumen recalculado para {len(periodos)} período(s).")
            except Exception as e:
                st.error(f"Error: {e}")

# ── Sección 2b: Seed de Clasificaciones (solo Checkpoint) ─────────────────────
with st.expander("🔄 Cargar clasificaciones (solo para detección de Checkpoint)"):
    st.caption("Usado únicamente para identificar materiales de tipo Checkpoint.")
    archivo_cl = st.file_uploader(
        "Seleccioná el Excel con hoja 'Clasificaciones'",
        type=["xlsx", "xls"],
        key="uploader_cl",
    )
    if st.button("Seed Clasificaciones", disabled=archivo_cl is None):
        with st.spinner("Cargando clasificaciones..."):
            try:
                n_cl = etl.seed_clasificaciones(db.get_conn(), archivo_cl.read())
                st.success(f"✅ {n_cl} materiales cargados.")
                periodos = db.get_periodos()
                if periodos:
                    with st.spinner(f"Recalculando {len(periodos)} período(s)..."):
                        for p in periodos:
                            etl.build_resumen(db.get_conn(), p)
                    st.info(f"Recalculado para {len(periodos)} período(s).")
            except Exception as e:
                st.error(f"Error: {e}")

# ── Sección 3: Equivalencias WL (precios nuevos) ──────────────────────────────
st.divider()
st.subheader("Equivalencias WL — precios nuevos")
st.caption(
    "Archivo **2026-Eq Materiales WL.xlsx** con hojas **ACTUALIZADO MAT PROD** (equivalencias) "
    "y **Precios** (ACV nuevo). Se usa para calcular ACV Anual/Mensual Nuevo por cliente."
)

n_equiv = len(db.get_equiv_wl())
if n_equiv > 0:
    st.info(f"Ya hay **{n_equiv} equivalencias WL** cargadas.")

with st.expander("🔄 Cargar equivalencias y precios WL (reemplaza las existentes)"):
    st.warning("Esto reemplazará las equivalencias y precios WL actuales y recalculará todos los períodos.")
    archivo_wl = st.file_uploader(
        "Seleccioná el Excel de equivalencias WL",
        type=["xlsx"],
        key="uploader_wl",
    )
    if st.button("Seed Equivalencias WL", disabled=archivo_wl is None):
        with st.spinner("Cargando equivalencias y precios..."):
            try:
                n_eq, n_pr = etl.seed_equiv_wl(db.get_conn(), archivo_wl.read())
                st.success(f"✅ {n_eq} equivalencias y {n_pr} precios WL cargados.")
                periodos = db.get_periodos()
                if periodos:
                    with st.spinner(f"Recalculando {len(periodos)} período(s)..."):
                        for p in periodos:
                            etl.build_resumen(db.get_conn(), p)
                    st.info(f"Resumen recalculado para {len(periodos)} período(s).")
            except Exception as e:
                st.error(f"Error: {e}")

# ── Sección 4: Detalle de uso ─────────────────────────────────────────────────
st.divider()
st.subheader("Detalle de uso — carga mensual")
st.caption("Archivo con solapas **USO SIL** y **USO LLN**. Ejemplo: `Cartera Legal - Detalle de uso 202602.xlsx`")

archivo_uso = st.file_uploader(
    "Seleccioná el Excel de detalle de uso",
    type=["xlsx"],
    key="uploader_uso",
)

periodo_uso_default = ""
if archivo_uso:
    m = re.search(r"(\d{4})(\d{2})", archivo_uso.name)
    if m:
        periodo_uso_default = f"{m.group(1)}-{m.group(2)}"

periodo_uso = st.text_input(
    "Período del uso (YYYY-MM)",
    value=periodo_uso_default,
    placeholder="2026-02",
    key="periodo_uso",
)

if st.button("⚡ Procesar uso", type="primary", disabled=(archivo_uso is None or not periodo_uso)):
    with st.spinner("Procesando uso..."):
        try:
            df_uso = etl.load_uso(archivo_uso.read())
            db.save_uso_periodo(df_uso, periodo_uso)
            n_sil = int((df_uso["uso_sil"] > 0).sum())
            n_lln = int((df_uso["uso_lln"] > 0).sum())
            st.success(
                f"✅ Uso cargado para **{periodo_uso}**: "
                f"{n_sil} clientes con eventos SIL, {n_lln} con eventos LLN"
            )
            db.log_upload("uso", periodo_uso, len(df_uso))
            st.toast(f"Uso {periodo_uso} cargado", icon="✅")
        except Exception as e:
            st.error(f"Error al procesar uso: {e}")

# ── Sección 4: Aging (deuda) ──────────────────────────────────────────────────
st.divider()
st.subheader("Aging / Deuda — carga mensual")
st.caption(
    "Archivo de aging con columnas **Customer Number**, **Deuda > 90**, **Deuda > 180**, **Over 360 Days**. "
    "Ejemplo: `Aging 202602.xlsx`"
)

archivo_aging = st.file_uploader(
    "Seleccioná el Excel de Aging",
    type=["xlsx"],
    key="uploader_aging",
)

periodo_aging_default = ""
if archivo_aging:
    m = re.search(r"(\d{4})(\d{2})", archivo_aging.name)
    if m:
        periodo_aging_default = f"{m.group(1)}-{m.group(2)}"

periodo_aging = st.text_input(
    "Período del aging (YYYY-MM)",
    value=periodo_aging_default,
    placeholder="2026-02",
    key="periodo_aging",
)

if st.button("⚡ Procesar aging", type="primary", disabled=(archivo_aging is None or not periodo_aging)):
    with st.spinner("Procesando aging..."):
        try:
            df_aging = etl.load_aging(archivo_aging.read())
            db.save_aging_periodo(df_aging, periodo_aging)
            n_deuda = int((df_aging["deuda_90"] > 0).sum())
            st.success(
                f"✅ Aging cargado para **{periodo_aging}**: "
                f"{len(df_aging):,} clientes procesados, {n_deuda:,} con deuda > 90 días"
            )
            db.log_upload("aging", periodo_aging, len(df_aging))
            st.toast(f"Aging {periodo_aging} cargado", icon="✅")
        except Exception as e:
            st.error(f"Error al procesar aging: {e}")

# ── Historial de cargas ────────────────────────────────────────────────────────
st.divider()
st.subheader("Historial de cargas")

log = db.get_upload_log()
if log.empty:
    st.caption("Sin cargas registradas.")
else:
    st.dataframe(log, use_container_width=True, hide_index=True)
