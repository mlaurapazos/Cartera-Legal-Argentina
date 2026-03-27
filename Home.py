"""
Home.py — Cartera Legal Analytics
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import db

st.set_page_config(page_title="Cartera Legal Analytics", layout="wide", page_icon="📚")

db.init_db()

# ── Estilos ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
body { font-family: Segoe UI, Arial, sans-serif; }
h1 { color: #1a3a5c; border-bottom: 3px solid #c8102e; padding-bottom: 8px; }
.card { background: #f0f4fa; border-radius: 8px; padding: 16px 20px;
        border-left: 4px solid #1a3a5c; margin-bottom: 12px; }
.card.ok   { border-left-color: #2d7d46; }
.card.warn { border-left-color: #dc8c14; }
.card.miss { border-left-color: #c8102e; background: #fff5f5; }
.card-val  { font-size: 1.5em; font-weight: bold; color: #1a3a5c; }
.card-lbl  { font-size: .83em; color: #555; margin-top: 4px; }
[data-testid="stSidebar"] { background: #1a3a5c; }
[data-testid="stSidebar"] * { color: #fff !important; }
</style>
""", unsafe_allow_html=True)

st.markdown(
    "<h1>📚 Cartera Legal Analytics</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "<p style='color:#666;font-size:.9em'>Thomson Reuters · CX Argentina · "
    "La Ley Next / SIL — Suscripciones Activas</p>",
    unsafe_allow_html=True,
)

# ── Estado de datos ───────────────────────────────────────────────────────────
periodos = db.get_periodos()
n_clases = len(db.get_clasificaciones())

col1, col2, col3 = st.columns(3)

with col1:
    if periodos:
        ultimo = periodos[0]
        res = db.get_resumen(ultimo)
        acv = res["total_acv_ars"].sum()
        st.markdown(
            f"<div class='card ok'>"
            f"<div class='card-val'>{len(res):,} clientes</div>"
            f"<div class='card-lbl'>Último período cargado: <strong>{ultimo}</strong><br>"
            f"ACV total: <strong>$ {acv:,.0f}</strong></div></div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div class='card miss'><div class='card-val'>Sin datos</div>"
            "<div class='card-lbl'>Cargá el primer período en <strong>Carga de Datos</strong></div></div>",
            unsafe_allow_html=True,
        )

with col2:
    clase_status = "ok" if n_clases > 0 else "miss"
    clase_txt = f"{n_clases} materiales clasificados" if n_clases > 0 else "Sin clasificaciones"
    st.markdown(
        f"<div class='card {clase_status}'>"
        f"<div class='card-val'>{n_clases}</div>"
        f"<div class='card-lbl'>{clase_txt}<br>"
        f"Editables en <strong>Clasificaciones</strong></div></div>",
        unsafe_allow_html=True,
    )

with col3:
    n_periodos = len(periodos)
    st.markdown(
        f"<div class='card'>"
        f"<div class='card-val'>{n_periodos} período{'s' if n_periodos != 1 else ''}</div>"
        f"<div class='card-lbl'>Historial cargado<br>"
        f"{'Comparación disponible en <strong>Evolución</strong>' if n_periodos > 1 else 'Cargá más períodos para comparar'}"
        f"</div></div>",
        unsafe_allow_html=True,
    )

# ── KPIs si hay datos ─────────────────────────────────────────────────────────
if periodos:
    st.divider()
    ultimo = periodos[0]
    res = db.get_resumen(ultimo)

    acv_total    = res["total_acv_ars"].sum()
    acv_mensual  = res["valor_mensual_ars"].sum()
    avg_cliente  = res["total_acv_ars"].mean()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Clientes activos", f"{len(res):,}")
    k2.metric("ACV Total (ARS)", f"$ {acv_total:,.0f}")
    k3.metric("Facturación mensual (ARS)", f"$ {acv_mensual:,.0f}")
    k4.metric("ACV promedio por cliente", f"$ {avg_cliente:,.0f}")

    # ── Tabla de períodos ─────────────────────────────────────────────────────
    st.subheader("Períodos cargados")
    rows = []
    for p in periodos:
        r = db.get_resumen(p)
        rows.append({
            "Período": p,
            "Clientes": len(r),
            "ACV Total ARS": r["total_acv_ars"].sum(),
            "Facturación mensual ARS": r["valor_mensual_ars"].sum(),
        })
    df_p = pd.DataFrame(rows)
    st.dataframe(
        df_p.style.format({
            "ACV Total ARS": "$ {:,.0f}",
            "Facturación mensual ARS": "$ {:,.0f}",
            "Clientes": "{:,}",
        }),
        use_container_width=True, hide_index=True,
    )

# ── Instrucciones ─────────────────────────────────────────────────────────────
with st.expander("📋 Instrucciones de uso"):
    st.markdown("""
**Primera vez:**
1. Ir a **Clasificaciones** → verificar que los materiales están cargados (si no, ir a Carga de Datos y usar el botón *Seed Clasificaciones*)
2. Ir a **Carga de Datos** → subir el Excel mensual → ingresar el período (YYYY-MM) → procesar

**Actualización mensual:**
1. Ir a **Carga de Datos** → subir el nuevo Excel → ingresar el período → procesar
2. El período anterior se conserva en el historial para comparación

**Archivo esperado:** `Analisis de suscripciones activas.xlsx`
Debe tener dos hojas: *Suscripciones activas MMYYYY* y *Clasificaciones*
""")
