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

col_f8, = st.columns([1])
with col_f8:
    tiene_papel_col = "tiene_papel" in df.columns
    if tiene_papel_col:
        filtro_papel = st.radio(
            "Papel",
            ["Todos", "Con papel", "Sin papel"],
            horizontal=True,
        )
    else:
        filtro_papel = "Todos"

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

# Filtro por diferencia ACV anual
tiene_dif_col = "acv_dif_anual" in df.columns and df["acv_dif_anual"].notna().any()
if tiene_dif_col:
    col_d1, col_d2 = st.columns([3, 1])
    with col_d1:
        dif_vals = df["acv_dif_anual"].fillna(0)
        min_dif = float(dif_vals.min())
        max_dif = float(dif_vals.max())
        if max_dif > min_dif:
            rango_dif = st.slider(
                "Diferencia ACV Anual (ARS)",
                min_value=min_dif,
                max_value=max_dif,
                value=(min_dif, max_dif),
                format="$ %,.0f",
                step=max(1.0, round((max_dif - min_dif) / 1000, 0)),
            )
        else:
            rango_dif = (min_dif, max_dif)
            st.caption(f"Diferencia ACV: $ {min_dif:,.0f}")
    with col_d2:
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption(f"$ {rango_dif[0]:,.0f} — $ {rango_dif[1]:,.0f}")
else:
    rango_dif = None

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
if tiene_papel_col and filtro_papel == "Con papel":
    df = df[df["tiene_papel"] == 1]
elif tiene_papel_col and filtro_papel == "Sin papel":
    df = df[df["tiene_papel"] != 1]
if busqueda:
    df = df[df["account_name"].astype(str).str.upper().str.contains(busqueda.upper(), na=False)]
df = df[(df["valor_mensual_ars"] >= rango_mens[0]) & (df["valor_mensual_ars"] <= rango_mens[1])]
if rango_dif is not None and tiene_dif_col:
    df = df[(df["acv_dif_anual"] >= rango_dif[0]) & (df["acv_dif_anual"] <= rango_dif[1])]

# ── Tabla de clientes ─────────────────────────────────────────────────────────
st.subheader(f"Detalle de clientes — {len(df):,} registros")

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
        return "background-color: #d4edda; color: #155724; font-weight: bold"
    if v < 0:
        return "background-color: #f8d7da; color: #721c24; font-weight: bold"
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
    "cant_usuarios":                 "Usuarios",
    "cant_tematicas":                "Temáticas",
    "cant_bibliotecas":              "Bibliotecas",
    "cant_revistas":                 "Revistas",
    "tiene_checkpoint":              "Checkpoint",
    "tiene_papel":                   "Papel",
    "producto_principal_suscripto":  "Prod. Principal",
    "uso_sil":                       "Uso SIL",
    "uso_lln":                       "Uso LLN",
    "no_uso":                        "No utiliza el producto",
    "deuda_90":                      "Deuda > 90",
    "deuda_180":                     "Deuda > 180",
    "deuda_360":                     "Deuda > 360",
}).copy()

COLS_ORDER = [
    "SAP", "Cliente", "Prod. SF", "Prod. Principal",
    "ACV Actual Anual", "ACV Actual Mensual",
    "ACV Anual Nuevo", "ACV Mensual Nuevo", "ACV Dif. Anual", "ACV Dif. Mensual",
    "Facturación", "Usuarios", "Papel", "Temáticas", "Bibliotecas", "Revistas", "Checkpoint",
    "Uso SIL", "Uso LLN", "No utiliza el producto",
    "Deuda > 90", "Deuda > 180", "Deuda > 360",
]
display = display[[c for c in COLS_ORDER if c in display.columns]]

display["Checkpoint"] = display["Checkpoint"].apply(lambda x: "✅" if x in (1, True) else "—")
if "Papel" in display.columns:
    display["Papel"] = display["Papel"].apply(lambda x: "✅" if x in (1, True) else "—")
if "No utiliza el producto" in display.columns:
    display["No utiliza el producto"] = display["No utiliza el producto"].apply(lambda x: "✅" if x in (1, True) else "—")

fmt = {}
for col in ["ACV Actual Anual", "ACV Actual Mensual", "ACV Anual Nuevo", "ACV Mensual Nuevo"]:
    if col in display.columns:
        fmt[col] = "$ {:,.0f}"
for col in ["ACV Dif. Anual", "ACV Dif. Mensual"]:
    if col in display.columns:
        fmt[col] = "$ {:+,.0f}"
if "Usuarios" in display.columns:
    fmt["Usuarios"] = "{:,.0f}"
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
st.dataframe(styled, use_container_width=True, hide_index=True, height=600)

# ── Descarga ──────────────────────────────────────────────────────────────────
buf = io.BytesIO()
display.to_excel(buf, index=False, sheet_name="Cartera")
st.download_button(
    "⬇️ Descargar Excel filtrado",
    data=buf.getvalue(),
    file_name=f"cartera_{periodo}_filtrada.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
