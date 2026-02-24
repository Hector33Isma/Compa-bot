# -*- coding: utf-8 -*-
import argparse
import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH_DEFAULT = "compat_db.sqlite"

IN_HEADERS = [
    "UserProductID","TITULO","SKU","FABRICANTE","MODELO","AÑO",
    "SUBMODELO","LITROS","CILINDROS","CARROCERÍA","TIPO DE TRANSMISIÓN",
    "TIPO DE TRACCIÓN","TIPO DE COMBUSTIBLE","TIPO DE MOTOR",
    "TIPO DE ASPIRACIÓN",
    "ASIGNACIÓN DE POSICIÓN(Conductor, Acompañante, Izquierda, Derecha, Delantera, Trasera, Interno, Externo, Superior, Inferior, Intermedio, Centro)",
    "NOTAS"
]
ASIG_COL = "ASIGNACIÓN DE POSICIÓN(Conductor, Acompañante, Izquierda, Derecha, Delantera, Trasera, Interno, Externo, Superior, Inferior, Intermedio, Centro)"
CLONAR_HEADERS = ["Publicación origen", "UserProduct origen", "UserProduct destino", "Incluir notas"]

def _is_blank(x) -> bool:
    if x is None: return True
    s = str(x).strip()
    return s == "" or s.lower() in {"nan","<na>","na","none","null","-"}

def _to_text(x) -> str:
    return "" if _is_blank(x) else str(x).strip()

def read_input(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    req_min = ["UserProductID", "TITULO", "SKU", "AÑO"]
    missing = [c for c in req_min if c not in df.columns]
    if missing:
        raise SystemExit(f"El archivo de entrada debe incluir columnas mínimas: {', '.join(req_min)}. Faltan: {', '.join(missing)}")
    for c in IN_HEADERS:
        if c not in df.columns: df[c] = ""
    df[IN_HEADERS] = df[IN_HEADERS].fillna("")
    for c in ["UserProductID","TITULO","SKU","FABRICANTE","MODELO","AÑO"]:
        df[c] = df[c].astype(str).str.strip()
    return df

def read_pos_normalizer(path: Path) -> dict:
    pdf = pd.read_excel(path, dtype=str, engine="openpyxl")
    pdf.columns = [str(c).strip() for c in pdf.columns]
    if not {"Entrada","Salida"}.issubset(set(pdf.columns)):
        raise SystemExit("El archivo de posiciones debe tener columnas: Entrada | Salida")
    m = {}
    for _, r in pdf.iterrows():
        k = ("" if pd.isna(r["Entrada"]) else str(r["Entrada"]).strip().upper())
        v = ("" if pd.isna(r["Salida"])  else str(r["Salida"]).strip())
        if k:
            m[k] = v
    return m

def read_multi_normalizer(path: Path) -> dict[str, list[str]]:
    """Soporta 1→N: varias filas con la misma Entrada producen varias Salidas."""
    df = pd.read_excel(path, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    if not {"Entrada","Salida"}.issubset(set(df.columns)):
        raise SystemExit("El normalizador debe tener columnas: Entrada | Salida")
    out: dict[str, list[str]] = {}
    for _, r in df.iterrows():
        ent = ("" if pd.isna(r["Entrada"]) else str(r["Entrada"]).strip().upper())
        sal = ("" if pd.isna(r["Salida"])  else str(r["Salida"]).strip())
        if not ent or _is_blank(sal):
            continue
        out.setdefault(ent, [])
        if sal not in out[ent]:
            out[ent].append(sal)
    return out

def normalize_one(term: str, mapa_norm: dict) -> list[str]:
    if _is_blank(term): return []
    raw = str(term).strip()
    key = raw.upper()
    mapped = mapa_norm.get(key, raw)  # si no existe en normalizador → tal cual
    if _is_blank(mapped): return []
    parts = [p.strip() for p in str(mapped).split(",") if not _is_blank(p)]
    return parts

def build_asignacion(lado, p1, p2, p3, mapa_pos: dict) -> str:
    tokens = []
    for t in [lado, p1, p2, p3]:
        for z in normalize_one(t, mapa_pos):
            if z not in tokens: tokens.append(z)
    return ", ".join(tokens)

def split_tokens(s: str) -> list[str]:
    """Divide un campo DB por coma o slash y limpia."""
    if _is_blank(s): return []
    raw = str(s)
    tokens = []
    for chunk in raw.replace("/", ",").split(","):
        t = chunk.strip()
        if t and t not in tokens:
            tokens.append(t)
    return tokens

def normalize_multi_from_db(value: str, mapping_multi: dict[str, list[str]]) -> list[str]:
    """
    Divide por '/' y ','.
    Si el token está en el normalizador:
      - Si su lista de salida está vacía => eliminar (no poner nada)
      - Si tiene salidas => devolverlas todas
    Si no está en el normalizador => dejar el token tal cual
    """
    if _is_blank(value):
        return []
    raw = str(value)
    tokens = []
    # Separa por slash o coma
    for chunk in raw.replace("/", ",").split(","):
        t = chunk.strip()
        if not t or t.lower() in {"nan", "none", "<na>", "-"}:
            continue
        tokens.append(t)

    outs: list[str] = []
    for t in tokens:
        key = t.upper()
        mapped = mapping_multi.get(key)
        if mapped is not None:
            # Si está en normalizador y su salida está vacía => eliminar (no agregar)
            if len(mapped) == 0:
                continue
            for m in mapped:
                if m and m not in outs:
                    outs.append(m)
        else:
            # No está en normalizador → conservar tal cual
            if t not in outs:
                outs.append(t)
    return outs


def query_compat(conn: sqlite3.Connection, sku: str) -> pd.DataFrame:
    q = """
        SELECT
            marca       AS FABRICANTE,
            modelo      AS MODELO,
            anio_inicio AS ANIO_INICIO,
            anio_fin    AS ANIO_FIN,
            lado, posicion_1, posicion_2, posicion_3,
            carroceria, traccion
        FROM compat
        WHERE sku = ?
        ORDER BY FABRICANTE, MODELO, ANIO_INICIO
    """
    return pd.read_sql_query(q, conn, params=(sku,))

def expand_years(row) -> list[int]:
    ai = int(row["ANIO_INICIO"]); af = int(row["ANIO_FIN"])
    if af < ai: af = ai
    return list(range(ai, af+1))

def generar_agregar_clonar_sin(
    input_path: Path,
    db_path: Path,
    pos_normalizer_path: Path,
    carroceria_normalizer_path: Path,
    traccion_normalizer_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, int]:
    conn = sqlite3.connect(db_path)
    try:
        df_in = read_input(input_path)
        mapa_pos = read_pos_normalizer(pos_normalizer_path)
        mapa_car = read_multi_normalizer(carroceria_normalizer_path)
        mapa_trac = read_multi_normalizer(traccion_normalizer_path)

        first_id_by_sku: dict[str, str] = {}
        agregar_rows, clonar_rows, sin_datos_rows = [], [], []

        for _, r in df_in.iterrows():
            sku = _to_text(r.get("SKU"))
            if not sku:
                continue

            origen_id = first_id_by_sku.get(sku)
            compat = query_compat(conn, sku)

            if compat.empty:
                sin_datos_rows.append({h: _to_text(r.get(h)) for h in IN_HEADERS})
                continue

            current_user_product_id = _to_text(r.get("UserProductID"))
            if origen_id is None:
                first_id_by_sku[sku] = current_user_product_id

                for _, c in compat.iterrows():
                    years = expand_years(c)
                    asignacion = build_asignacion(
                        c.get("lado"),
                        c.get("posicion_1"),
                        c.get("posicion_2"),
                        c.get("posicion_3"),
                        mapa_pos,
                    )

                    cars = normalize_multi_from_db(c.get("carroceria"), mapa_car) or [""]
                    tracs = normalize_multi_from_db(c.get("traccion"), mapa_trac) or [""]

                    for y in years:
                        for car in cars:
                            for tr in tracs:
                                out = {h: "" for h in IN_HEADERS}
                                out["UserProductID"] = current_user_product_id
                                out["TITULO"] = _to_text(r.get("TITULO"))
                                out["SKU"] = sku
                                out["FABRICANTE"] = _to_text(c.get("FABRICANTE")).upper()
                                out["MODELO"] = _to_text(c.get("MODELO")).upper()
                                out["AÑO"] = f"{int(y):04d}"
                                out["CARROCERÍA"] = _to_text(car)
                                out["TIPO DE TRACCIÓN"] = _to_text(tr)
                                out[ASIG_COL] = _to_text(asignacion)
                                agregar_rows.append(out)
            else:
                clonar_rows.append({
                    "Publicación origen": "",
                    "UserProduct origen": origen_id,
                    "UserProduct destino": current_user_product_id,
                    "Incluir notas": "Sí",
                })

        df_agregar = pd.DataFrame(agregar_rows, columns=IN_HEADERS).fillna("")
        df_clonar = pd.DataFrame(clonar_rows, columns=CLONAR_HEADERS).fillna("")
        df_sin = pd.DataFrame(sin_datos_rows, columns=IN_HEADERS).fillna("")
        return df_agregar, df_clonar, df_sin, len(df_in)
    finally:
        conn.close()

def write_xlsx(path: Path, df: pd.DataFrame):
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.fillna("").to_excel(w, index=False)

def main():
    ap = argparse.ArgumentParser(
        description="Genera Agregar/Clonar/Sin_datos incluyendo ASIGNACIÓN DE POSICIÓN y normalización de CARROCERÍA/TRACCIÓN (1→N)."
    )
    ap.add_argument("--input", required=True, help="Publicaciones (XLSX).")
    ap.add_argument("--db", default=DB_PATH_DEFAULT, help="Ruta a compat_db.sqlite.")
    ap.add_argument("--pos-normalizer", default="normalizador_posiciones.xlsx", help="XLSX posiciones: Entrada/Salida.")
    ap.add_argument("--carroceria-normalizer", default="normalizador_carroceria.xlsx", help="XLSX carrocería: Entrada/Salida (1→N).")
    ap.add_argument("--traccion-normalizer", default="normalizador_traccion.xlsx", help="XLSX tracción: Entrada/Salida (1→N).")
    ap.add_argument("--out-agregar", default="Agregar_compatibilidades.xlsx")
    ap.add_argument("--out-clonar", default="Clonar_compatibilidades.xlsx")
    ap.add_argument("--out-sin", default="Sin_datos.xlsx")
    args = ap.parse_args()

    df_agregar, df_clonar, df_sin, _ = generar_agregar_clonar_sin(
        input_path=Path(args.input),
        db_path=Path(args.db),
        pos_normalizer_path=Path(args.pos_normalizer),
        carroceria_normalizer_path=Path(args.carroceria_normalizer),
        traccion_normalizer_path=Path(args.traccion_normalizer),
    )

    write_xlsx(Path(args.out_agregar), df_agregar)
    write_xlsx(Path(args.out_clonar), df_clonar)

    if not df_sin.empty:
        write_xlsx(Path(args.out_sin), df_sin)

    print(f"[OK] Agregar: {len(df_agregar)} | Clonar: {len(df_clonar)} | Sin_datos: {len(df_sin)}")
    print(f" -> {args.out_agregar}\n -> {args.out_clonar}")
    if not df_sin.empty:
        print(f" -> {args.out_sin}")

if __name__ == "__main__":
    main()
