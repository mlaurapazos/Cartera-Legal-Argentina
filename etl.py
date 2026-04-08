"""
etl.py — Lógica de procesamiento Cartera Legal Analytics
"""
import pandas as pd
import numpy as np
from io import BytesIO

import db

# Materiales Westlaw excluyentes: solo el de mayor valor por cliente
WL_EXCLUSIVOS = {"43570954", "43570951", "43572801"}

# Mapeo PRODUC (nueva estructura) → nombre de producto y prioridad
PRODUC_PRODUCTO = {
    "FULL": "TR Full",
    "PROF": "TR Profesional",
}
PRODUC_PRIORIDAD = {"TR Full": 1, "TR Profesional": 2}


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
    """Siembra clasificaciones (usado solo para detección de Checkpoint)."""
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


def seed_estructura(conn, file_bytes: bytes) -> int:
    """Siembra la tabla estructura desde la hoja 'LISTADO GRAL (2)' del Excel de planes."""
    df = pd.read_excel(BytesIO(file_bytes), sheet_name="LISTADO GRAL (2)")
    df = df[df["MATERIAL"].notna()].copy()

    def _safe_code(x):
        try:
            return str(int(float(x)))
        except (ValueError, TypeError):
            return None

    df["material"] = df["MATERIAL"].apply(_safe_code)
    df = df[df["material"].notna()]
    df["descripcion"] = df["DESCRIPCION"].astype(str).str.strip()
    df["formato"]     = df["FORMATO"].astype(str).str.strip()
    df["tem_gen"]     = df["TEM/GEN"].astype(str).str.strip().replace({"nan": None})
    df["produc"]      = df["PRODUC"].astype(str).str.strip().replace({"nan": None})
    df["lln_sil"]     = df["LLN/SIL"].astype(str).str.strip().replace({"nan": None})

    df = df[["material", "descripcion", "formato", "tem_gen", "produc", "lln_sil"]].drop_duplicates("material")
    db.save_estructura(df)
    return len(df)


def seed_equiv_wl(conn, file_bytes: bytes) -> tuple:
    """Siembra equiv_wl y precios_wl desde el Excel 2026-Eq Materiales WL."""
    def _safe_code(x):
        if pd.isna(x):
            return None
        try:
            return str(int(float(x)))
        except (ValueError, TypeError):
            return None

    # Equivalencias (filas 0-1 son títulos, datos desde fila 2)
    eq = pd.read_excel(BytesIO(file_bytes), sheet_name="ACTUALIZADO MAT PROD", header=None)
    eq = eq.iloc[2:].copy()
    eq_clean = pd.DataFrame({
        "mat_actual":  eq.iloc[:, 0].apply(_safe_code),
        "mat_nuevo_1": eq.iloc[:, 7].apply(_safe_code),
        "mat_nuevo_2": eq.iloc[:, 9].apply(_safe_code),
        "mat_nuevo_3": eq.iloc[:, 11].apply(_safe_code),
    })
    eq_clean = eq_clean[eq_clean["mat_actual"].notna()].drop_duplicates("mat_actual")
    db.save_equiv_wl(eq_clean)

    # Precios (fila 0 es header, datos desde fila 1)
    pr = pd.read_excel(BytesIO(file_bytes), sheet_name="Precios", header=None)
    pr = pr.iloc[1:].copy()
    pr_clean = pd.DataFrame({
        "material":    pr.iloc[:, 0].apply(_safe_code),
        "acv_anual":   pd.to_numeric(pr.iloc[:, 2], errors="coerce"),
        "acv_mensual": pd.to_numeric(pr.iloc[:, 3], errors="coerce"),
    })
    pr_clean = pr_clean[pr_clean["material"].notna()].drop_duplicates("material")
    db.save_precios_wl(pr_clean)

    return len(eq_clean), len(pr_clean)


def _calc_acv_nuevo(df: pd.DataFrame, equiv: pd.DataFrame, precios: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula ACV nuevo por cliente aplicando equivalencias WL.
    Reglas:
    - Cada material nuevo se suma UNA SOLA VEZ por cliente.
    - Los 3 materiales WL exclusivos (ADVANCED ONLINE, +PAPEL, CORE) son excluyentes:
      si un cliente tiene varios, se queda solo con el de mayor valor.
    """
    precios_dict = {
        str(row["material"]): {
            "acv_anual":   float(row["acv_anual"]   or 0),
            "acv_mensual": float(row["acv_mensual"] or 0),
        }
        for _, row in precios.iterrows()
        if pd.notna(row["material"])
    }

    equiv_lookup: dict[str, list[str]] = {}
    for _, row in equiv.iterrows():
        mat = str(row["mat_actual"]) if pd.notna(row["mat_actual"]) else None
        if not mat:
            continue
        nuevos = [
            str(n) for n in [row.get("mat_nuevo_1"), row.get("mat_nuevo_2"), row.get("mat_nuevo_3")]
            if pd.notna(n) and n is not None
        ]
        equiv_lookup[mat] = nuevos

    results = []
    for sold_to_pt, g in df.groupby("sold_to_pt"):
        materiales = set(g["mat_code"].dropna().unique())

        # Recopilar todos los materiales nuevos únicos
        nuevos: set[str] = set()
        for mat in materiales:
            for nuevo in equiv_lookup.get(mat, []):
                nuevos.add(nuevo)

        # Exclusión Westlaw: conservar solo el de mayor valor
        wl_candidatos = nuevos & WL_EXCLUSIVOS
        if len(wl_candidatos) > 1:
            mejor_wl = max(
                wl_candidatos,
                key=lambda m: precios_dict.get(m, {}).get("acv_anual", 0),
            )
            nuevos = (nuevos - WL_EXCLUSIVOS) | {mejor_wl}

        acv_anual   = sum(precios_dict.get(m, {}).get("acv_anual",   0) for m in nuevos)
        acv_mensual = sum(precios_dict.get(m, {}).get("acv_mensual", 0) for m in nuevos)

        results.append({
            "sold_to_pt":        sold_to_pt,
            "acv_anual_nuevo":   round(acv_anual,   2),
            "acv_mensual_nuevo": round(acv_mensual, 2),
        })

    return pd.DataFrame(results) if results else pd.DataFrame(
        columns=["sold_to_pt", "acv_anual_nuevo", "acv_mensual_nuevo"]
    )


def load_aging(file_bytes: bytes) -> pd.DataFrame:
    """
    Lee el Excel de Aging (deuda por cliente).
    Devuelve DataFrame con sold_to_pt, deuda_90, deuda_180, deuda_360.
    - Deuda > 90  ← columna "Deuda > 90" del archivo
    - Deuda > 180 ← columna "Deuda > 180" del archivo
    - Deuda > 360 ← columna "Over 360 Days" del archivo
    """
    df = pd.read_excel(BytesIO(file_bytes))

    df = df[df["Customer Number"].notna()].copy()
    df["sold_to_pt"] = df["Customer Number"].apply(lambda x: str(int(float(x))))

    def _col(df, name):
        return pd.to_numeric(df[name], errors="coerce").fillna(0) if name in df.columns else 0

    df["deuda_90"]  = _col(df, "Deuda > 90")
    df["deuda_180"] = _col(df, "Deuda > 180")
    df["deuda_360"] = _col(df, "Over 360 Days")

    return df.groupby("sold_to_pt").agg(
        deuda_90=("deuda_90", "sum"),
        deuda_180=("deuda_180", "sum"),
        deuda_360=("deuda_360", "sum"),
    ).reset_index()


def load_uso(file_bytes: bytes) -> pd.DataFrame:
    """
    Lee el Excel de detalle de uso (solapas USO SIL y USO LLN).
    Devuelve un DataFrame con sold_to_pt, uso_sil, uso_lln (total de eventos por cliente).
    """
    uso_sil = pd.read_excel(BytesIO(file_bytes), sheet_name="USO SIL")
    uso_lln = pd.read_excel(BytesIO(file_bytes), sheet_name="USO LLN")

    sil_counts = (
        uso_sil["m-user-sap_customer_number"]
        .dropna()
        .astype(str).str.strip()
        .value_counts()
    )
    lln_counts = (
        uso_lln["SAP ID"]
        .dropna()
        .astype(str).str.strip()
        .value_counts()
    )

    df_sil = sil_counts.rename("uso_sil").reset_index()
    df_sil.columns = ["sold_to_pt", "uso_sil"]

    df_lln = lln_counts.rename("uso_lln").reset_index()
    df_lln.columns = ["sold_to_pt", "uso_lln"]

    merged = df_sil.merge(df_lln, on="sold_to_pt", how="outer")
    merged["uso_sil"] = merged["uso_sil"].fillna(0).astype(int)
    merged["uso_lln"] = merged["uso_lln"].fillna(0).astype(int)
    return merged


def build_resumen(conn, periodo: str) -> int:
    """
    Lee raw_suscripciones + estructura (nueva) + clasificaciones (Checkpoint),
    calcula resumen por cliente y lo persiste en resumen_mensual.
    """
    df  = db.get_raw_suscripciones()
    est = db.get_estructura()
    cl  = db.get_clasificaciones()

    # ── Join suscripciones ← estructura (por código numérico de material) ──────
    def _safe_code(x):
        if pd.isna(x):
            return None
        try:
            return str(int(float(x)))
        except (ValueError, TypeError):
            return None

    df["mat_code"] = df["material"].apply(_safe_code)

    if not est.empty:
        est_join = est.rename(columns={"material": "mat_code"})
        df = df.merge(est_join[["mat_code", "formato", "tem_gen", "produc"]], on="mat_code", how="left")
    else:
        df["formato"] = None
        df["tem_gen"] = None
        df["produc"]  = None

    # ── Checkpoint desde clasificaciones (join por descripción, igual que antes) ─
    cl["mat_norm"] = cl["material"].astype(str).str.strip().str.upper()
    checkpoint_mats = set(cl[cl["producto_principal"].astype(str) == "Checkpoint"]["mat_norm"])
    df["mat_desc_norm"] = df["material_desc"].astype(str).str.strip().str.upper()
    df["es_checkpoint"] = df["mat_desc_norm"].isin(checkpoint_mats)

    df["acv_ars"]      = pd.to_numeric(df["acv_ars"],      errors="coerce")
    df["billing_value"] = pd.to_numeric(df["billing_value"], errors="coerce")

    def agg_cliente(g):
        account_name = g["account_name"].dropna().iloc[0] if g["account_name"].notna().any() else None
        prod_sf      = g["producto_principal_sf"].dropna().iloc[0] if g["producto_principal_sf"].notna().any() else None

        # Materiales únicos, sin HighQ
        mat_unicos = g.drop_duplicates("mat_code")
        mat_unicos = mat_unicos[
            ~mat_unicos["material_desc"].astype(str).str.upper().str.contains("HIGHQ|HIGH-Q", na=False)
        ]

        total_acv     = mat_unicos["acv_ars"].sum()
        total_billing = mat_unicos["billing_value"].sum()
        tipo_facturacion = "Anual" if abs(total_acv - total_billing) < 1 else "Mensual"
        valor_mensual    = round(total_acv / 12, 2)

        # ── Cantidad de usuarios: mínimo entre todos los materiales con valor ─
        todos_usuarios = pd.to_numeric(g["cant_usuarios"], errors="coerce").dropna()
        todos_usuarios = todos_usuarios[todos_usuarios > 0]
        cant_usuarios = int(todos_usuarios.min()) if not todos_usuarios.empty else None

        # ── Temáticas: FORMATO == "BSUB" Y TEM/GEN == "TEM" ──────────────────
        n_tematicas = int(((mat_unicos["formato"] == "BSUB") & (mat_unicos["tem_gen"] == "TEM")).sum())
        # ── Bibliotecas: FORMATO en ["PV", "BIB"] ─────────────────────────────
        n_bibliotecas = int(mat_unicos["formato"].isin(["PV", "BIB"]).sum())
        # ── Revistas: FORMATO en ["Online", "Papel"] ──────────────────────────
        n_revistas = int(mat_unicos["formato"].isin(["Online", "Papel"]).sum())
        # ── Checkpoint ────────────────────────────────────────────────────────
        tiene_checkpoint = bool(mat_unicos["es_checkpoint"].any())

        # ── Producto Principal: BSUB con PRODUC conocido, mayor prioridad ─────
        bsub_mats = mat_unicos[mat_unicos["formato"] == "BSUB"]
        prod_suscripto = None
        candidatos = [
            (PRODUC_PRIORIDAD[PRODUC_PRODUCTO[p]], PRODUC_PRODUCTO[p])
            for p in bsub_mats["produc"].dropna()
            if p in PRODUC_PRODUCTO
        ]
        if candidatos:
            prod_suscripto = min(candidatos, key=lambda x: x[0])[1]

        # Fallback si no hay BSUB con PRODUC conocido
        if prod_suscripto is None:
            if n_tematicas > 0 and n_bibliotecas > 0:
                prod_suscripto = "Temáticas / Bibliotecas"
            elif n_tematicas > 0:
                prod_suscripto = "Temáticas"
            elif n_bibliotecas > 0:
                prod_suscripto = "Bibliotecas"
            elif tiene_checkpoint:
                prod_suscripto = "Checkpoint"

        return pd.Series({
            "account_name":                 account_name,
            "producto_principal_sf":        prod_sf,
            "total_acv_ars":                round(total_acv, 2),
            "valor_mensual_ars":            valor_mensual,
            "cant_usuarios":                cant_usuarios,
            "cant_tematicas":               n_tematicas,
            "cant_bibliotecas":             n_bibliotecas,
            "cant_revistas":                n_revistas,
            "tiene_checkpoint":             int(tiene_checkpoint),
            "producto_principal_suscripto": prod_suscripto,
            "tipo_facturacion":             tipo_facturacion,
        })

    resumen = df.groupby("sold_to_pt").apply(agg_cliente, include_groups=False).reset_index()
    resumen = resumen.sort_values("total_acv_ars", ascending=False)

    # ── ACV Nuevo (equivalencias WL) ──────────────────────────────────────────
    equiv   = db.get_equiv_wl()
    precios = db.get_precios_wl()
    if not equiv.empty and not precios.empty:
        acv_nuevo = _calc_acv_nuevo(df, equiv, precios)
        resumen = resumen.merge(acv_nuevo, on="sold_to_pt", how="left")
        resumen["acv_anual_nuevo"]   = resumen["acv_anual_nuevo"].fillna(0).round(2)
        resumen["acv_mensual_nuevo"] = resumen["acv_mensual_nuevo"].fillna(0).round(2)
        resumen["acv_dif_anual"]     = (resumen["total_acv_ars"]    - resumen["acv_anual_nuevo"]).round(2)
        resumen["acv_dif_mensual"]   = (resumen["valor_mensual_ars"] - resumen["acv_mensual_nuevo"]).round(2)
    else:
        resumen["acv_anual_nuevo"]   = None
        resumen["acv_mensual_nuevo"] = None
        resumen["acv_dif_anual"]     = None
        resumen["acv_dif_mensual"]   = None

    db.save_resumen_periodo(resumen, periodo)
    return len(resumen)
