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

# Columna derivada: no utiliza el producto (uso SIL + uso LLN == 0)
tiene_uso = "uso_sil" in df.columns and "uso_lln" in df.columns
if tiene_uso:
    df["no_uso"] = ((df["uso_sil"].fillna(0) + df["uso_lln"].fillna(0)) == 0)

# Columnas de deuda
tiene_aging = "deuda_90" in df.columns

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
    solo_sin_uso = st.checkbox("Solo sin uso", disabled=not tiene_uso)

with col_f4:
    busqueda = st.text_input("Buscar cliente", placeholder="Nombre...")

col_f5, col_f6, col_f7 = st.columns([2, 3, 3])
with col_f5:
    sel_facturacion = st.multiselect(
        "Tipo de facturación", ["Anual", "Mensual"], default=[]
    )
with col_f6:
    if tiene_aging:
        filtro_deuda = st.radio(
            "Deuda (> 90 días)",
            ["Todos", "Con deuda", "Sin deuda"],
            horizontal=True,
        )
    else:
        filtro_deuda = "Todos"
with col_f7:
    tiene_dif = "acv_dif_anual" in df.columns
    if tiene_dif:
        filtro_dif = st.radio(
            "Diferencia ACV",
            ["Todos", "🟢 Pagan más (sube)", "🔴 Pagan menos (baja)"],
            horizontal=True,
        )
    else:
        filtro_dif = "Todos"

# Filtro por importe mensual
col_r1, col_r2 = st.columns([3, 1])
with col_r1:
    vals = df["valor_mensual_ars"].fillna(0)
    min_mens = float(vals.min())
    max_mens = float(vals.max())
    if max_mens > min_mens:
        rango_mens = st.slider(
            "Facturación mensual (ARS)",
            min_value=min_mens,
            max_value=max_mens,
            value=(min_mens, max_mens),
            format="$ %,.0f",
            step=max(1.0, round((max_mens - min_mens) / 1000, 0)),
        )
    else:
        rango_mens = (min_mens, max_mens)
        st.caption(f"Facturación mensual: $ {min_mens:,.0f}")
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
if solo_sin_uso and tiene_uso:
    df = df[df["no_uso"] == True]
if sel_facturacion:
    df = df[df["tipo_facturacion"].isin(sel_facturacion)]
if tiene_aging and filtro_deuda == "Con deuda":
    df = df[df["deuda_90"] > 0]
elif tiene_aging and filtro_deuda == "Sin deuda":
    df = df[df["deuda_90"] <= 0]
if tiene_dif and filtro_dif == "🟢 Pagan más (sube)":
    df = df[df["acv_dif_anual"] > 0]
elif tiene_dif and filtro_dif == "🔴 Pagan menos (baja)":
    df = df[df["acv_dif_anual"] < 0]
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

if tiene_aging:
    d1, d2, d3 = st.columns(3)
    n_d90  = int((df["deuda_90"]  > 0).sum())
    n_d180 = int((df["deuda_180"] > 0).sum())
    n_d360 = int((df["deuda_360"] > 0).sum())
    d1.metric("Clientes Deuda > 90d",  f"{n_d90:,}",  f"{n_d90/len(df)*100:.0f}%"  if len(df) else "—")
    d2.metric("Clientes Deuda > 180d", f"{n_d180:,}", f"{n_d180/len(df)*100:.0f}%" if len(df) else "—")
    d3.metric("Clientes Deuda > 360d", f"{n_d360:,}", f"{n_d360/len(df)*100:.0f}%" if len(df) else "—")

if "uso_sil" in df.columns or "uso_lln" in df.columns:
    u1, u2, u3 = st.columns(3)
    if "uso_sil" in df.columns:
        n_sil = int((df["uso_sil"] > 0).sum())
        u1.metric("Clientes con uso SIL", f"{n_sil:,}", f"{n_sil/len(df)*100:.0f}%" if len(df) else "—")
    if "uso_lln" in df.columns:
        n_lln = int((df["uso_lln"] > 0).sum())
        u2.metric("Clientes con uso LLN", f"{n_lln:,}", f"{n_lln/len(df)*100:.0f}%" if len(df) else "—")
    if "uso_sil" in df.columns:
        u3.metric("Total eventos SIL", f"{int(df['uso_sil'].sum()):,}")

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

def color_dif(val):
    try:
        v = float(val)
    except (TypeError, ValueError):
        return ""
    if v > 0:
        return "background-color: #f8d7da; color: #721c24; font-weight: bold"
    if v < 0:
        return "background-color: #d4edda; color: #155724; font-weight: bold"
    return ""

display = df.rename(columns={
    "sold_to_pt":                    "SAP",
    "account_name":                  "Cliente",
    "producto_principal_sf":         "Prod. SF",
    "total_acv_ars":                 "ACV Actual Anual",
    "valor_mensual_ars":             "ACV Actual Mensual",
    "acv_anual_nuevo":               "ACV Anual Nuevo",
    "acv_mensual_nuevo":             "ACV Mensual Nuevo",
    "acv_dif_anual":                 "ACV Dif. Anual",
    "acv_dif_mensual":               "ACV Dif. Mensual",
    "tipo_facturacion":              "Facturación",
    "cant_tematicas":                "Temáticas",
    "cant_bibliotecas":              "Bibliotecas",
    "cant_revistas":                 "Revistas",
    "tiene_checkpoint":              "Checkpoint",
    "producto_principal_suscripto":  "Prod. Principal",
    "uso_sil":                       "Uso SIL",
    "uso_lln":                       "Uso LLN",
    "no_uso":                        "No utiliza el producto",
    "deuda_90":                      "Deuda > 90",
    "deuda_180":                     "Deuda > 180",
    "deuda_360":                     "Deuda > 360",
}).copy()

# Orden explícito de columnas
COLS_ORDER = [
    "SAP", "Cliente", "Prod. SF", "Prod. Principal",
    "ACV Actual Anual", "ACV Actual Mensual",
    "ACV Anual Nuevo", "ACV Mensual Nuevo", "ACV Dif. Anual", "ACV Dif. Mensual",
    "Facturación", "Temáticas", "Bibliotecas", "Revistas", "Checkpoint",
    "Uso SIL", "Uso LLN", "No utiliza el producto",
    "Deuda > 90", "Deuda > 180", "Deuda > 360",
]
display = display[[c for c in COLS_ORDER if c in display.columns]]

display["Checkpoint"] = display["Checkpoint"].map({1: "✅", 0: "—", True: "✅", False: "—"})
if "No utiliza el producto" in display.columns:
    display["No utiliza el producto"] = display["No utiliza el producto"].map({True: "✅", False: "—"})

fmt = {}
for col in ["ACV Actual Anual", "ACV Actual Mensual", "ACV Anual Nuevo", "ACV Mensual Nuevo"]:
    if col in display.columns:
        fmt[col] = "$ {:,.0f}"
for col in ["ACV Dif. Anual", "ACV Dif. Mensual"]:
    if col in display.columns:
        fmt[col] = "$ {:+,.0f}"
if "Uso SIL" in display.columns:
    fmt["Uso SIL"] = "{:,}"
if "Uso LLN" in display.columns:
    fmt["Uso LLN"] = "{:,}"
for col_deuda in ["Deuda > 90", "Deuda > 180", "Deuda > 360"]:
    if col_deuda in display.columns:
        fmt[col_deuda] = "$ {:,.0f}"

dif_cols = [c for c in ["ACV Dif. Anual", "ACV Dif. Mensual"] if c in display.columns]

styled = (
    display.style
    .map(color_prod, subset=["Prod. Principal"])
    .map(color_dif, subset=dif_cols if dif_cols else ["ACV Dif. Anual"])
    .format(fmt, na_rep="—")
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
