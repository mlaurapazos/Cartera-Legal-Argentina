"""
3_Evolucion.py — Comparación mes a mes de la cartera
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import db

st.set_page_config(page_title="Evolución · Cartera Legal", layout="wide")

st.markdown(
    "<h1 style='color:#1a3a5c;border-bottom:3px solid #c8102e;padding-bottom:8px'>"
    "📈 Evolución de la Cartera</h1>",
    unsafe_allow_html=True,
)

periodos = db.get_periodos()
if len(periodos) < 2:
    st.warning("Se necesitan al menos **2 períodos** cargados para ver la evolución.")
    if len(periodos) == 1:
        st.info(f"Solo hay 1 período cargado: **{periodos[0]}**. Cargá otro mes en **Carga de Datos**.")
    st.stop()

# ── Tabla de evolución general ────────────────────────────────────────────────
st.subheader("Evolución por período")

rows = []
for p in sorted(periodos):
    r = db.get_resumen(p)
    rows.append({
        "Período": p,
        "Clientes": len(r),
        "ACV Total ARS": r["total_acv_ars"].sum(),
        "Facturación mensual ARS": r["valor_mensual_ars"].sum(),
    })
evol = pd.DataFrame(rows)
evol_sorted = evol.sort_values("Período")

# Deltas vs período anterior
evol_sorted["Δ Clientes"] = evol_sorted["Clientes"].diff().fillna(0).astype(int)
evol_sorted["Δ ACV ARS"]  = evol_sorted["ACV Total ARS"].diff().fillna(0).round(0)

st.dataframe(
    evol_sorted.style.format({
        "ACV Total ARS":          "$ {:,.0f}",
        "Facturación mensual ARS": "$ {:,.0f}",
        "Δ ACV ARS":              "{:+,.0f}",
        "Clientes":               "{:,}",
        "Δ Clientes":             "{:+,}",
    }),
    use_container_width=True, hide_index=True,
)

# ── Charts ────────────────────────────────────────────────────────────────────
col_c1, col_c2 = st.columns(2)
with col_c1:
    st.markdown("**ACV Total por período (ARS)**")
    st.line_chart(evol_sorted.set_index("Período")["ACV Total ARS"], height=250)
with col_c2:
    st.markdown("**Clientes activos por período**")
    st.line_chart(evol_sorted.set_index("Período")["Clientes"], height=250)

# ── Altas y bajas entre dos períodos ──────────────────────────────────────────
st.divider()
st.subheader("Altas y bajas entre dos períodos")

col_p1, col_p2 = st.columns(2)
with col_p1:
    p_base = st.selectbox("Período base (anterior)", periodos[1:], index=0)
with col_p2:
    p_comp = st.selectbox("Período de comparación (más reciente)", periodos, index=0)

if p_base == p_comp:
    st.warning("Seleccioná dos períodos distintos.")
    st.stop()

r_base = db.get_resumen(p_base).set_index("sold_to_pt")
r_comp = db.get_resumen(p_comp).set_index("sold_to_pt")

saps_base = set(r_base.index)
saps_comp = set(r_comp.index)

altas = saps_comp - saps_base
bajas = saps_base - saps_comp

col_a, col_b = st.columns(2)

with col_a:
    st.markdown(f"**🟢 Altas en {p_comp}** ({len(altas)} clientes nuevos)")
    if altas:
        df_altas = r_comp.loc[list(altas), ["account_name", "producto_principal_suscripto", "total_acv_ars"]].reset_index()
        df_altas = df_altas.rename(columns={"sold_to_pt": "SAP", "account_name": "Cliente",
                                             "producto_principal_suscripto": "Producto",
                                             "total_acv_ars": "ACV ARS"})
        st.dataframe(
            df_altas.style.format({"ACV ARS": "$ {:,.0f}"}),
            use_container_width=True, hide_index=True, height=300,
        )
    else:
        st.caption("Sin altas entre estos períodos.")

with col_b:
    st.markdown(f"**🔴 Bajas en {p_comp}** ({len(bajas)} clientes que salieron)")
    if bajas:
        df_bajas = r_base.loc[list(bajas), ["account_name", "producto_principal_suscripto", "total_acv_ars"]].reset_index()
        df_bajas = df_bajas.rename(columns={"sold_to_pt": "SAP", "account_name": "Cliente",
                                             "producto_principal_suscripto": "Producto",
                                             "total_acv_ars": "ACV ARS"})
        st.dataframe(
            df_bajas.style.format({"ACV ARS": "$ {:,.0f}"}),
            use_container_width=True, hide_index=True, height=300,
        )
    else:
        st.caption("Sin bajas entre estos períodos.")

# ── ACV ganado y perdido ──────────────────────────────────────────────────────
acv_altas = r_comp.loc[list(altas), "total_acv_ars"].sum() if altas else 0
acv_bajas = r_base.loc[list(bajas), "total_acv_ars"].sum() if bajas else 0
acv_neto  = acv_altas - acv_bajas

st.divider()
m1, m2, m3 = st.columns(3)
m1.metric("ACV ganado (altas)", f"$ {acv_altas:,.0f}")
m2.metric("ACV perdido (bajas)", f"$ {acv_bajas:,.0f}", delta_color="inverse")
m3.metric("ACV neto", f"$ {acv_neto:+,.0f}", delta=f"{acv_neto:+,.0f}",
          delta_color="normal" if acv_neto >= 0 else "inverse")
