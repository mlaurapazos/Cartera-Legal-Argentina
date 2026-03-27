"""
2_Cartera.py — Dashboard principal de cartera activa
"""
import io
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import db

st.set_page_config(page_title="Cartera · Cartera Legal", layout="wide")

st.markdown(
    "<h1 style='color:#1a3a5c;border-bottom:3px solid #c8102e;padding-bottom:8px'>"
    "📋 Cartera de Clientes</h1>",
    unsafe_allow_html=True,
)

periodos = db.get_periodos()
if not periodos:
    st.warning("Sin datos. Cargá el primer período en **Carga de Datos**.")
    st.stop()

# ── Selector de período ───────────────────────────────────────────────────────
periodo = st.selectbox("Período", periodos, index=0)
df = db.get_resumen(periodo)

# ── Filtros ───────────────────────────────────────────────────────────────────
st.markdown("#### Filtros")
col_f1, col_f2, col_f3, col_f4 = st.columns([2, 2, 1, 2])

with col_f1:
    prods_sf = sorted(df["producto_principal_sf"].dropna().unique())
    sel_sf = st.multiselect("Producto Principal SF", prods_sf, default=[])

with col_f2:
    prods_sus = sorted(df["producto_principal_suscripto"].dropna().unique())
    sel_sus = st.multiselect("Producto Principal Suscripto", prods_sus, default=[])

with col_f3:
    solo_ck = st.checkbox("Solo Checkpoint")

with col_f4:
    busqueda = st.text_input("Buscar cliente", placeholder="Nombre...")

# Filtro por importe mensual
col_r1, col_r2 = st.columns([3, 1])
with col_r1:
    min_mens = float(df["valor_mensual_ars"].min())
    max_mens = float(df["valor_mensual_ars"].max())
    rango_mens = st.slider(
        "Facturación mensual (ARS)",
        min_value=min_mens,
        max_value=max_mens,
        value=(min_mens, max_mens),
        format="$ %,.0f",
        step=max(1.0, round((max_mens - min_mens) / 1000, 0)),
    )
with col_r2:
    st.markdown("<br>", unsafe_allow_html=True)
    st.caption(f"$ {rango_mens[0]:,.0f} — $ {rango_mens[1]:,.0f}")

# Aplicar filtros
if sel_sf:
    df = df[df["producto_principal_sf"].isin(sel_sf)]
if sel_sus:
    df = df[df["producto_principal_suscripto"].isin(sel_sus)]
if solo_ck:
    df = df[df["tiene_checkpoint"] == 1]
if busqueda:
    df = df[df["account_name"].astype(str).str.upper().str.contains(busqueda.upper(), na=False)]
df = df[(df["valor_mensual_ars"] >= rango_mens[0]) & (df["valor_mensual_ars"] <= rango_mens[1])]

# ── KPI cards ─────────────────────────────────────────────────────────────────
acv_total   = df["total_acv_ars"].sum()
mens_total  = df["valor_mensual_ars"].sum()
avg_cliente = df["total_acv_ars"].mean() if len(df) else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Clientes", f"{len(df):,}")
k2.metric("ACV Total (ARS)", f"$ {acv_total:,.0f}")
k3.metric("Facturación mensual (ARS)", f"$ {mens_total:,.0f}")
k4.metric("ACV promedio por cliente", f"$ {avg_cliente:,.0f}")

# ── Gráfico distribución ──────────────────────────────────────────────────────
st.subheader("Distribución por Producto Principal Suscripto")
dist = (
    df.groupby("producto_principal_suscripto")
    .agg(clientes=("sold_to_pt", "count"), acv=("total_acv_ars", "sum"))
    .reset_index()
    .sort_values("acv", ascending=False)
)
col_chart1, col_chart2 = st.columns(2)
with col_chart1:
    st.bar_chart(dist.set_index("producto_principal_suscripto")["clientes"], height=280)
    st.caption("Cantidad de clientes")
with col_chart2:
    st.bar_chart(dist.set_index("producto_principal_suscripto")["acv"], height=280)
    st.caption("ACV total (ARS)")

# ── Tabla de clientes ─────────────────────────────────────────────────────────
st.subheader(f"Detalle de clientes — {len(df):,} registros")

# Colores por producto principal suscripto
COLORES_PROD = {
    "TR Full":                "#1a3a5c",
    "TR Profesional":         "#2d7d46",
    "TR Practica":            "#5c7a2d",
    "TR Duo":                 "#80a53b",
    "Temáticas":              "#c8802e",
    "Temáticas / Bibliotecas": "#dc8c14",
    "Bibliotecas":            "#6b4c9a",
    "Checkpoint":             "#c8102e",
}

def color_prod(val):
    color = COLORES_PROD.get(str(val), "#888")
    return f"background-color: {color}; color: white; font-weight: bold; text-align: center"

display = df.rename(columns={
    "sold_to_pt":                    "SAP",
    "account_name":                  "Cliente",
    "producto_principal_sf":         "Prod. SF",
    "total_acv_ars":                 "ACV ARS",
    "valor_mensual_ars":             "Mensual ARS",
    "cant_tematicas":                "Temáticas",
    "cant_bibliotecas":              "Bibliotecas",
    "cant_revistas":                 "Revistas",
    "tiene_checkpoint":              "Checkpoint",
    "producto_principal_suscripto":  "Prod. Principal",
}).copy()

display["Checkpoint"] = display["Checkpoint"].map({1: "✅", 0: "—", True: "✅", False: "—"})

styled = (
    display.style
    .applymap(color_prod, subset=["Prod. Principal"])
    .format({"ACV ARS": "$ {:,.0f}", "Mensual ARS": "$ {:,.0f}"})
)
st.dataframe(styled, use_container_width=True, hide_index=True, height=500)

# ── Descarga ──────────────────────────────────────────────────────────────────
buf = io.BytesIO()
display.to_excel(buf, index=False, sheet_name="Cartera")
st.download_button(
    "⬇️ Descargar Excel filtrado",
    data=buf.getvalue(),
    file_name=f"cartera_{periodo}_filtrada.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
