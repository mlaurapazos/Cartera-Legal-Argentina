"""
etl.py — Lógica de procesamiento Cartera Legal Analytics
"""
import pandas as pd
import numpy as np
from io import BytesIO

import db

PRIORIDAD = {"TR Full": 1, "TR Profesional": 2, "TR Practica": 3, "TR Duo": 4}


def load_suscripciones(file_bytes: bytes) -> pd.DataFrame:
    """Lee el Excel mensual de suscripciones activas (formato Salesforce, header en fila 2)."""
    df = pd.read_excel(BytesIO(file_bytes), header=2)

    # Renombrar columnas a nombres internos fijos
    col_map = {
        "Fecha": "fecha",
        "Account Name": "account_name",
        "Producto Principal": "producto_principal_sf",
        "Producto": "producto",
        "Sold-to pt": "sold_to_pt",
        "Large Account": "large_account",
        "Cant_Usuarios": "cant_usuarios",
        "Customer Class": "customer_class",
        "Customer Group": "customer_group",
        "Sector": "sector",
        "Subsector": "subsector",
        "Industria LatAm": "industria_latam",
        "Sub Industria Latam": "sub_industria_latam",
        "País": "pais", "Pa\u00eds": "pais", "Pais": "pais",
        "Ciudad": "ciudad",
        "Type SF": "type_sf",
        "Tax Number SF": "tax_number",
        "Material": "material",
        "Material Desc": "material_desc",
        "ACV_ARS": "acv_ars",
        "Max ACV": "max_acv",
        "Billing_Value": "billing_value",
        "BU2": "bu2",
    }
    df = df.rename(columns={c: col_map.get(c, c) for c in df.columns})

    # Filtrar footer de Salesforce (filas sin SAP numérico válido)
    def es_sap_valido(x):
        if pd.isna(x):
            return False
        try:
            int(float(x))
            return True
        except (ValueError, TypeError):
            return False

    df = df[df["sold_to_pt"].apply(es_sap_valido)].copy()
    df["sold_to_pt"] = df["sold_to_pt"].apply(lambda x: str(int(float(x))))
    df["acv_ars"] = pd.to_numeric(df["acv_ars"], errors="coerce")
    df["fecha"] = df["fecha"].astype(str)
    return df


def seed_clasificaciones(conn, file_bytes: bytes):
    """Siembra la tabla clasificaciones desde la hoja 'Clasificaciones' del Excel."""
    cl = pd.read_excel(BytesIO(file_bytes), sheet_name="Clasificaciones")
    cl = cl.rename(columns={
        "Material": "material",
        "Es principal": "es_principal_raw",
        "Producto Principal": "producto_principal",
    })
    cl["material"] = cl["material"].astype(str).str.strip()
    cl["es_principal"] = cl["es_principal_raw"].astype(str).str.strip().str.upper().str.startswith("S").astype(int)
    cl["producto_principal"] = cl["producto_principal"].astype(str).str.strip().replace("nan", None)
    cl = cl[["material", "es_principal", "producto_principal"]].drop_duplicates("material")
    db.save_clasificaciones(cl)
    return len(cl)


def _producto_principal_suscripto(tipos_principales: pd.Series, n_tematicas: int,
                                   n_bibliotecas: int, tiene_checkpoint: bool) -> str | None:
    """Determina el producto principal con prioridad y fallback."""
    candidatos = [t for t in tipos_principales.dropna() if t in PRIORIDAD]
    if candidatos:
        return min(candidatos, key=lambda t: PRIORIDAD[t])
    # Fallback
    if n_tematicas > 0 and n_bibliotecas > 0:
        return "Temáticas / Bibliotecas"
    if n_tematicas > 0:
        return "Temáticas"
    if n_bibliotecas > 0:
        return "Bibliotecas"
    if tiene_checkpoint:
        return "Checkpoint"
    return None


def build_resumen(conn, periodo: str) -> int:
    """
    Lee raw_suscripciones + clasificaciones, calcula resumen por cliente,
    y lo persiste en resumen_mensual para el período dado.
    Retorna el número de clientes procesados.
    """
    df = db.get_raw_suscripciones()
    cl = db.get_clasificaciones()

    # Normalizar para join
    df["mat_norm"] = df["material_desc"].astype(str).str.strip().str.upper()
    cl["mat_norm"] = cl["material"].astype(str).str.strip().str.upper()
    cl["es_principal"] = cl["es_principal"].astype(bool)
    cl["tipo"] = cl["producto_principal"].astype(str).str.strip().replace("nan", None)

    df = df.merge(cl[["mat_norm", "tipo", "es_principal"]], on="mat_norm", how="left")
    df["tipo"] = df["tipo"].where(df["tipo"] != "None", None)
    df["acv_ars"] = pd.to_numeric(df["acv_ars"], errors="coerce")
    df["billing_value"] = pd.to_numeric(df["billing_value"], errors="coerce")

    def agg_cliente(g):
        account_name = g["account_name"].dropna().iloc[0] if g["account_name"].notna().any() else None
        prod_sf = g["producto_principal_sf"].dropna().iloc[0] if g["producto_principal_sf"].notna().any() else None

        # Materiales únicos, sin HighQ
        mat_unicos = g.drop_duplicates("mat_norm")
        mat_unicos = mat_unicos[~mat_unicos["mat_norm"].str.contains("HIGHQ|HIGH-Q", na=False)]

        total_acv     = mat_unicos["acv_ars"].sum()
        total_billing = mat_unicos["billing_value"].sum()

        # Tipo de facturación: Anual si billing ≈ ACV (±5%), Mensual si billing << ACV
        if total_acv > 0 and abs(total_billing - total_acv) / total_acv <= 0.05:
            tipo_facturacion = "Anual"
            valor_mensual = round(total_acv / 12, 2)
        else:
            tipo_facturacion = "Mensual"
            valor_mensual = round(total_billing, 2)

        mat_por_tipo = mat_unicos.groupby("tipo")["mat_norm"].count()
        n_tematicas   = int(mat_por_tipo.get("Tematica", 0))
        n_bibliotecas = int(mat_por_tipo.get("Bibliotecas", 0))
        n_revistas    = int(mat_por_tipo.get("Revista", 0))
        tiene_checkpoint = bool((g["tipo"] == "Checkpoint").any())

        tipos_principales = g.loc[g["es_principal"] == True, "tipo"]
        prod_suscripto = _producto_principal_suscripto(
            tipos_principales, n_tematicas, n_bibliotecas, tiene_checkpoint
        )

        return pd.Series({
            "account_name":                 account_name,
            "producto_principal_sf":        prod_sf,
            "total_acv_ars":                round(total_acv, 2),
            "valor_mensual_ars":            valor_mensual,
            "cant_tematicas":               n_tematicas,
            "cant_bibliotecas":             n_bibliotecas,
            "cant_revistas":                n_revistas,
            "tiene_checkpoint":             int(tiene_checkpoint),
            "producto_principal_suscripto": prod_suscripto,
            "tipo_facturacion":             tipo_facturacion,
        })

    resumen = df.groupby("sold_to_pt").apply(agg_cliente, include_groups=False).reset_index()
    resumen = resumen.sort_values("total_acv_ars", ascending=False)

    db.save_resumen_periodo(resumen, periodo)
    return len(resumen)
