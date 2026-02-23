# -*- coding: utf-8 -*-
import argparse
import sqlite3
from pathlib import Path
import pandas as pd
import re
import math

DB_PATH_DEFAULT = "compat_db.sqlite"

# === utilidades ===
def norm_text(x: str) -> str:
    if x is None:
        return ""
    return str(x).strip()

def is_na_like(x) -> bool:
    if x is None:
        return True
    if x is pd.NA:
        return True
    if isinstance(x, float) and math.isnan(x):
        return True
    s = str(x).strip().lower()
    return s in {"", "nan", "<na>", "na", "none", "null", "-"}

def norm_text_nullable(x):
    """Devuelve None si es vacío/NA-like, si no regresa el texto limpio."""
    if is_na_like(x):
        return None
    return str(x).strip()

def parse_year(x):
    if x is None:
        return None
    m = re.search(r"(\d{4})", str(x))
    if not m:
        return None
    y = int(m.group(1))
    if 1900 <= y <= 2100:
        return y
    return None

def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1].lower() for r in cur.fetchall()]
    return col.lower() in cols

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    # Tabla principal (incluye nuevas columnas carroceria/traccion)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS compat (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            marca TEXT NOT NULL,
            modelo TEXT NOT NULL,
            anio_inicio INTEGER NOT NULL,
            anio_fin INTEGER NOT NULL,
            lado TEXT,
            posicion_1 TEXT,
            posicion_2 TEXT,
            posicion_3 TEXT,
            carroceria TEXT,
            traccion TEXT
        )
    """)
    conn.commit()

    # Si ya existía sin las nuevas columnas, las añadimos
    if not _col_exists(conn, "compat", "carroceria"):
        cur.execute("ALTER TABLE compat ADD COLUMN carroceria TEXT")
    if not _col_exists(conn, "compat", "traccion"):
        cur.execute("ALTER TABLE compat ADD COLUMN traccion TEXT")
    conn.commit()

    # Índice único actual: si ya migraste a incluir lado/posiciones en la unicidad,
    # déjalo así; si no, puedes mantener el anterior. Aquí NO tocamos tu índice.
    # Ejemplo (comentado):  descomenta si quieres forzarlo aquí.
    # cur.execute("DROP INDEX IF EXISTS ux_compat_sku_marca_modelo_anios")
    # cur.execute("""
    #     CREATE UNIQUE INDEX IF NOT EXISTS ux_compat_full
    #     ON compat (sku, marca, modelo, anio_inicio, anio_fin, lado, posicion_1, posicion_2, posicion_3)
    # """)
    # conn.commit()

def insert_rows(conn: sqlite3.Connection, rows: list[dict]) -> tuple[int,int]:
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    for r in rows:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO compat
                (sku, marca, modelo, anio_inicio, anio_fin,
                 lado, posicion_1, posicion_2, posicion_3,
                 carroceria, traccion)
                VALUES (?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?)
            """, (
                r["SKU"], r["MARCA"], r["MODELO"], r["ANIO_INICIO"], r["ANIO_FIN"],
                r.get("LADO"), r.get("POSICION_1"), r.get("POSICION_2"), r.get("POSICION_3"),
                r.get("CARROCERIA"), r.get("TRACCION")
            ))
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    return inserted, skipped

def export_db_to_xlsx(conn: sqlite3.Connection, out_path: Path):
    sql = """
        SELECT
            sku          AS "SKU",
            marca        AS "MARCA",
            modelo       AS "MODELO",
            anio_inicio  AS "AÑO INICIO",
            anio_fin     AS "AÑO FIN",
            lado         AS "LADO",
            posicion_1   AS "POSICION_1",
            posicion_2   AS "POSICION_2",
            posicion_3   AS "POSICION_3",
            carroceria   AS "CARROCERIA",
            traccion     AS "TRACCION"
        FROM compat
        ORDER BY sku, marca, modelo, anio_inicio
    """
    df = pd.read_sql_query(sql, conn)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, index=False)

def main():
    ap = argparse.ArgumentParser(
        description="Ingesta de compatibilidades a SQLite (dedup por SKU,MARCA,MODELO,AÑO INICIO,AÑO FIN,[LADO y POSICION_1..3 si tu índice ya lo contempla])."
    )
    ap.add_argument("--input", required=True, help="Excel de desarrollo a ingerir (XLSX). Obligatorias: SKU, MARCA, MODELO, AÑO INICIO, AÑO FIN. Opcionales: LADO, POSICION_1..3, CARROCERIA, TRACCION")
    ap.add_argument("--db", default=DB_PATH_DEFAULT, help="Ruta a compat_db.sqlite (se crea si no existe).")
    ap.add_argument("--export-xlsx", help="(Opcional) Exportar lo que hay en DB a este XLSX.")
    args = ap.parse_args()

    path_in = Path(args.input)
    conn = sqlite3.connect(args.db)
    ensure_schema(conn)

    # Leer Excel forzando string
    df = pd.read_excel(path_in, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    required = ["SKU","MARCA","MODELO","AÑO INICIO","AÑO FIN"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Faltan columnas obligatorias: {missing}")

    # Normalizar/validar filas
    rows = []
    for _, r in df.iterrows():
        sku = norm_text(r.get("SKU"))
        marca = norm_text(r.get("MARCA"))
        modelo = norm_text(r.get("MODELO"))
        ai = parse_year(r.get("AÑO INICIO"))
        af = parse_year(r.get("AÑO FIN"))

        if not sku or not marca or not modelo or ai is None or af is None:
            continue  # fila inválida -> se salta en ingesta

        # Opcionales: posiciones
        lado = norm_text_nullable(r.get("LADO")) if "LADO" in df.columns else None
        p1   = norm_text_nullable(r.get("POSICION_1")) if "POSICION_1" in df.columns else None
        p2   = norm_text_nullable(r.get("POSICION_2")) if "POSICION_2" in df.columns else None
        p3   = norm_text_nullable(r.get("POSICION_3")) if "POSICION_3" in df.columns else None

        # Opcionales NUEVOS: carroceria / traccion  (se guardan canonicalizados a UPPER si tienen valor)
        carroceria = norm_text_nullable(r.get("CARROCERIA")) if "CARROCERIA" in df.columns else None
        traccion   = norm_text_nullable(r.get("TRACCION")) if "TRACCION" in df.columns else None

        rows.append({
            "SKU": sku,
            "MARCA": marca.upper(),
            "MODELO": modelo.upper(),
            "ANIO_INICIO": ai,
            "ANIO_FIN": af,
            "LADO": lado,
            "POSICION_1": p1,
            "POSICION_2": p2,
            "POSICION_3": p3,
            "CARROCERIA": carroceria.upper() if carroceria else None,
            "TRACCION": traccion.upper() if traccion else None,
        })

    ins, dup = insert_rows(conn, rows)
    print(f"[OK] Ingesta terminada. Insertados: {ins} | Ya existentes: {dup} | Total leídas: {len(rows)}")
    if args.export_xlsx:
        export_db_to_xlsx(conn, Path(args.export_xlsx))
        print(f"[OK] Exportado DB a: {args.export_xlsx}")

    conn.close()

if __name__ == "__main__":
    main()
