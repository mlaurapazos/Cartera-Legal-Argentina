"""
4_Clasificaciones.py — CRUD de materiales
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import db
import etl

st.set_page_config(page_title="Clasificaciones · Cartera Legal", layout="wide")

st.markdown(
    "<h1 style='color:#1a3a5c;border-bottom:3px solid #c8102e;padding-bottom:8px'>"
    "🏷️ Clasificaciones de Materiales</h1>",
    unsafe_allow_html=True,
)

st.markdown("""
Editá directamente en la tabla. Hacé clic en una celda para modificarla.
Al terminar, presioná **Guardar cambios**.

**Producto Principal** válidos: `TR Full`, `TR Profesional`, `TR Practica`, `TR Duo`,
`Tematica`, `Bibliotecas`, `Revista`, `Checkpoint`
""")

df_cl = db.get_clasificaciones()

if df_cl.empty:
    st.warning("No hay clasificaciones cargadas. Usá el botón en **Carga de Datos** para sembrar desde el Excel.")
    st.stop()

# Editor de datos
PRODUCTOS_VALIDOS = [
    "TR Full", "TR Profesional", "TR Practica", "TR Duo",
    "Tematica", "Bibliotecas", "Revista", "Checkpoint",
]

edited = st.data_editor(
    df_cl,
    use_container_width=True,
    hide_index=True,
    num_rows="dynamic",
    column_config={
        "material": st.column_config.TextColumn("Material", width="large"),
        "es_principal": st.column_config.CheckboxColumn("Es principal"),
        "producto_principal": st.column_config.SelectboxColumn(
            "Producto Principal",
            options=PRODUCTOS_VALIDOS,
            required=False,
        ),
    },
    key="editor_cl",
)

col_save, col_recalc = st.columns([1, 2])

with col_save:
    if st.button("💾 Guardar cambios", type="primary"):
        try:
            edited["es_principal"] = edited["es_principal"].fillna(False).astype(int)
            db.save_clasificaciones(edited)
            st.success("✅ Clasificaciones guardadas.")
            st.toast("Cambios guardados", icon="✅")
        except Exception as e:
            st.error(f"Error al guardar: {e}")

with col_recalc:
    periodos = db.get_periodos()
    if periodos:
        if st.button(f"🔄 Recalcular todos los períodos ({len(periodos)})"):
            edited["es_principal"] = edited["es_principal"].fillna(False).astype(int)
            db.save_clasificaciones(edited)
            with st.spinner("Recalculando..."):
                conn = db.get_conn()
                for p in periodos:
                    etl.build_resumen(conn, p)
            st.success(f"✅ Resumen recalculado para {len(periodos)} período(s).")
    else:
        st.caption("No hay períodos cargados aún.")

# ── Resumen de clasificaciones ────────────────────────────────────────────────
st.divider()
st.subheader("Resumen")

col1, col2 = st.columns(2)
with col1:
    st.markdown("**Por Producto Principal**")
    resumen = df_cl["producto_principal"].value_counts(dropna=False).reset_index()
    resumen.columns = ["Producto Principal", "Materiales"]
    st.dataframe(resumen, use_container_width=True, hide_index=True)

with col2:
    st.markdown("**Marcados como Principal**")
    n_si = int((df_cl["es_principal"].astype(str).str.strip().isin(["1","True","SI","Si","true"])).sum())
    n_no = len(df_cl) - n_si
    st.metric("Es principal = Sí", n_si)
    st.metric("Es principal = No", n_no)
