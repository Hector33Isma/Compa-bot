# -*- coding: utf-8 -*-
"""
normalizador_v2.py
Héctor • Normalizador de marcas/modelos/año con soporte de CARROCERÍA/TRACCIÓN.

Entrada (XLSX) columnas requeridas:
  UserProductID, TITULO, SKU, FABRICANTE, MODELO, AÑO
Col opcionales:
  SUBMODELO, LITROS, CILINDROS, CARROCERÍA, TIPO DE TRANSMISIÓN,
  TIPO DE TRACCIÓN, TIPO DE COMBUSTIBLE, TIPO DE MOTOR, TIPO DE ASPIRACIÓN,
  ASIGNACIÓN DE POSICIÓN(Conductor, Acompañante, Izquierda, Derecha, Delantera, Trasera, Interno, Externo, Superior, Inferior, Intermedio, Centro),
  NOTAS

Base de datos del normalizador (XLSX):
  mi_marca, mi_modelo, anio_ini, anio_fin, carroceria, traccion,
  ml_marca, ml_modelo, ml_carroceria, ml_traccion

Reglas:
  1) Se intenta primero combinación (mi_marca + mi_modelo).
  2) Si no, solo mi_marca (mi_modelo='*').
  3) Si no, solo mi_modelo (mi_marca='*').
  4) En todos los casos, si la fila BD especifica carrocería y/o tracción,
     deben coincidir con la entrada; si están vacías en BD, aplican a cualquiera.
  5) Años: si la fila BD tiene anio_ini/fin vacíos, aplica a cualquier año;
     si trae rango, solo aplica dentro del rango (inclusive).
  6) La salida reemplaza FABRICANTE/MODELO por ml_marca/ml_modelo cuando existan.
     También escribe CARROCERÍA/TIPO DE TRACCIÓN con ml_carroceria/ml_traccion
     si están pobladas; si no, respeta lo que traía la entrada.

Salida:
  - out_ok (XLSX): filas normalizadas.
  - out_err (XLSX): filas descartadas por faltar UserProductID/TITULO/SKU/AÑO.
"""

import argparse
from pathlib import Path
import re
import unicodedata

import pandas as pd
import polars as pl


# === Encabezado exacto de la columna de posiciones (columna P) ===
COL_ASIG = "ASIGNACIÓN DE POSICIÓN(Conductor, Acompañante, Izquierda, Derecha, Delantera, Trasera, Interno, Externo, Superior, Inferior, Intermedio, Centro)"

# === Columnas esperadas en ENTRADA (orden de salida) ===
REQ_COLS_IN = [
    "UserProductID","TITULO","SKU","FABRICANTE","MODELO","AÑO",
    "SUBMODELO","LITROS","CILINDROS","CARROCERÍA","TIPO DE TRANSMISIÓN",
    "TIPO DE TRACCIÓN","TIPO DE COMBUSTIBLE","TIPO DE MOTOR","TIPO DE ASPIRACIÓN",
    COL_ASIG,
    "NOTAS",
]

# === Columnas esperadas en BD de normalización ===
REQ_COLS_DB = [
    "mi_marca","mi_modelo","anio_ini","anio_fin",
    "carroceria","traccion",
    "ml_marca","ml_modelo","ml_carroceria","ml_traccion",
]


# ---------- utilidades ----------
def strip_accents(s: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFD", str(s))
        if unicodedata.category(ch) != "Mn"
    )


def norm_key(s: str) -> str:
    return strip_accents(str(s)).upper().strip()


def norm_key_opt(s: str | None) -> str:
    return norm_key(s) if s is not None else ""


def val_int_or_none(x):
    if x is None:
        return None
    m = re.search(r"(\d{4})", str(x))
    if not m:
        return None
    y = int(m.group(1))
    if 1900 <= y <= 2100:
        return y
    return None


def is_blank_like(x) -> bool:
    if x is None:
        return True
    s = str(x).strip().lower()
    return s in {"", "nan", "<na>", "none", "null", "-"}


def read_table(path: Path, expected_cols: list[str], csv_encoding: str = "utf8") -> pl.DataFrame:
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        pdf = pd.read_excel(path, dtype=str, engine="openpyxl")
        pdf.columns = [str(c).strip() for c in pdf.columns]
        for c in expected_cols:
            if c not in pdf.columns:
                pdf[c] = ""

        # >>> clave: NO convertir NaN a "nan". Primero llenar vacíos:
        pdf[expected_cols] = pdf[expected_cols].fillna("")

        # Ahora sí, como texto "limpio"
        for c in expected_cols:
            pdf[c] = pdf[c].astype(str)

        df = pl.from_pandas(pdf)

        # Cast explícito a Utf8 (string) y mantener orden
        df = df.select([pl.col(c).cast(pl.Utf8) for c in expected_cols] +
                       [c for c in df.columns if c not in expected_cols])
        ordered = [c for c in expected_cols] + [c for c in df.columns if c not in expected_cols]
        return df.select(ordered)
    else:
        raise ValueError(f"Formato no soportado: {ext}")

def write_xlsx(path: Path, df: pl.DataFrame):
    # Forzar texto y vacíos reales (no NaN)
    df_txt = (
        df
        .with_columns(pl.all().cast(pl.Utf8))
        .with_columns(pl.all().fill_null(""))
    )
    # Pasar a pandas y volver a asegurar vacíos
    pdf = df_txt.to_pandas()
    pdf = pdf.fillna("")  # por si algún NaN se coló

    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pdf.to_excel(w, index=False)

# ----------------------------------------------------------
# wc_match(pattern, value):
#   Usa '*' como comodín en mi_marca / mi_modelo:
#     "*"         → cualquier valor
#     "JETTA*"    → empieza con "JETTA"
#     "*JETTA*"   → contiene "JETTA"
#     "JE*TA"     → "JE" + lo que sea + "TA"
#
# Se aplica sobre key_marca / key_modelo ya normalizados (sin acentos, mayúsculas).
# La prioridad del normalizador sigue siendo:
#   1) mi_marca != "*" y mi_modelo != "*"
#   2) mi_marca != "*" y mi_modelo == "*"
#   3) mi_marca == "*" y mi_modelo != "*"
# ----------------------------------------------------------

def wc_match(pattern: str, value: str) -> bool:
    """
    Compara 'pattern' contra 'value' usando '*' como comodín:
      "*"         → cualquier valor
      "JETTA*"    → empieza con "JETTA"
      "*JETTA*"   → contiene "JETTA"
      "JE*TA"     → lo que sea entre "JE" y "TA"
    Ambos se asumen ya normalizados (sin acentos, mayúsculas).
    """
    p = (pattern or "").strip()
    v = (value or "").strip()
    if p == "":
        return v == ""
    # Convertir '*' a '.*' y armar regex anclado
    regex = "^" + re.escape(p).replace("\\*", ".*") + "$"
    return re.match(regex, v) is not None

# ---------- Normalizador ----------
class Normalizador:
    def __init__(self, df_db: pl.DataFrame):
        # Normaliza/deriva columnas clave para matching
        self.db = (
            df_db
            .with_columns([
                pl.col("mi_marca").map_elements(norm_key, return_dtype=pl.Utf8).alias("key_marca"),
                pl.col("mi_modelo").map_elements(norm_key, return_dtype=pl.Utf8).alias("key_modelo"),
                pl.col("carroceria").map_elements(norm_key_opt, return_dtype=pl.Utf8).alias("key_carroceria"),
                pl.col("traccion").map_elements(norm_key_opt, return_dtype=pl.Utf8).alias("key_traccion"),
                pl.col("anio_ini").map_elements(val_int_or_none, return_dtype=pl.Int64).alias("anio_ini"),
                pl.col("anio_fin").map_elements(val_int_or_none, return_dtype=pl.Int64).alias("anio_fin"),
            ])
        )

        # Particiones lógicas (comodines con '*')
        self.db_combo = self.db.filter(
            (pl.col("mi_marca") != "*") & (pl.col("mi_modelo") != "*")
        )
        self.db_solo_marca = self.db.filter(
            (pl.col("mi_marca") != "*") & (pl.col("mi_modelo") == "*")
        )
        self.db_solo_modelo = self.db.filter(
            (pl.col("mi_marca") == "*") & (pl.col("mi_modelo") != "*")
        )

    @staticmethod
    def _in_range(anio: int | None, ini: int | None, fin: int | None) -> bool:
        if ini is None and fin is None:
            return True
        if anio is None:
            return False
        if ini is None:
            ini = anio
        if fin is None:
            fin = anio
        return ini <= anio <= fin

    def _aplica_extra_filters(self, df: pl.DataFrame, k_car: str, k_trac: str) -> pl.DataFrame:
        """
        Si en la fila BD hay carrocería/tracción, debe coincidir con la entrada.
        Si están vacías en BD (''), aplican a cualquiera.
        """
        cond_car = ( (pl.col("key_carroceria") == "") | (pl.col("key_carroceria") == k_car) )
        cond_tr  = ( (pl.col("key_traccion")  == "") | (pl.col("key_traccion")  == k_trac) )
        return df.filter(cond_car & cond_tr)

    def _mk_out(self, fila_in: dict, row_db: dict, year: int | None):
        """
        Construye la fila de salida usando ml_* cuando existan.
        También respeta CARROCERÍA / TIPO DE TRACCIÓN desde ml_carroceria/ml_traccion si están pobladas.
        """
        # Año
        if isinstance(year, int):
            anio_out = f"{year:04d}"
        else:
            anio_out = str(fila_in.get("AÑO", "")).strip()

        # Partimos de la fila original
        out = dict(fila_in)

        # Marca/modelo normalizados si están definidos
        out_marca = row_db.get("ml_marca") or fila_in["FABRICANTE"]
        out_modelo = row_db.get("ml_modelo") or fila_in["MODELO"]

        out["FABRICANTE"] = out_marca
        out["MODELO"] = out_modelo
        out["AÑO"] = anio_out

        # Carrocería / Tracción desde ml_* si hay valor; si no, mantiene lo de entrada
        ml_car = row_db.get("ml_carroceria")
        ml_tr  = row_db.get("ml_traccion")

        out["CARROCERÍA"] = (ml_car if (ml_car is not None and str(ml_car).strip() != "") else fila_in.get("CARROCERÍA", ""))
        out["TIPO DE TRACCIÓN"] = (ml_tr if (ml_tr is not None and str(ml_tr).strip() != "") else fila_in.get("TIPO DE TRACCIÓN", ""))

        # SKU siempre como texto literal (no perder ceros ni guiones)
        sku_val = fila_in.get("SKU")
        out["SKU"] = "" if sku_val is None else str(sku_val)

        return out

    def transformar(self, fila: dict) -> list[dict]:
        """
        Devuelve 0..N filas normalizadas según las reglas.
        Puede devolver varias si la BD tiene múltiples ml_modelo para la misma combinación.
        """
        outs: list[dict] = []

        k_marca = norm_key(fila.get("FABRICANTE", ""))
        k_modelo = norm_key(fila.get("MODELO", ""))
        y = val_int_or_none(fila.get("AÑO"))
        k_car = norm_key_opt(fila.get("CARROCERÍA", ""))
        k_trac = norm_key_opt(fila.get("TIPO DE TRACCIÓN", ""))

         # 1) Combinación marca+modelo (soporta comodines en mi_marca / mi_modelo)
        matches = self._aplica_extra_filters(
            self._filter_combo_wc(k_marca, k_modelo),
            k_car, k_trac
        )
        if matches.height > 0:
            for row in matches.to_dicts():
                if self._in_range(y, row["anio_ini"], row["anio_fin"]):
                    outs.append(self._mk_out(fila, row, y))
            if outs:
                return outs  # si hubo match de combo, no seguimos a nivel marca/modelo

        # 2) Solo marca (mi_modelo='*') con comodín en mi_marca
        m2 = self._aplica_extra_filters(
            self._filter_solo_marca_wc(k_marca),
            k_car, k_trac
        )
        if m2.height > 0:
            for row in m2.to_dicts():
                if self._in_range(y, row["anio_ini"], row["anio_fin"]):
                    outs.append(self._mk_out(fila, row, y))
            if outs:
                return outs

        # 3) Solo modelo (mi_marca='*') con comodín en mi_modelo
        m3 = self._aplica_extra_filters(
            self._filter_solo_modelo_wc(k_modelo),
            k_car, k_trac
        )
        if m3.height > 0:
            for row in m3.to_dicts():
                if self._in_range(y, row["anio_ini"], row["anio_fin"]):
                    outs.append(self._mk_out(fila, row, y))
            if outs:
                return outs

        # 4) Si no hay ninguna coincidencia, devolver sin cambios
        return [self._mk_out(fila, {}, y)]
    
    def _filter_combo_wc(self, k_marca: str, k_modelo: str) -> pl.DataFrame:
        """
        Busca en self.db_combo (mi_marca != "*" y mi_modelo != "*")
        usando comodines en key_marca / key_modelo.
        """
        return self.db_combo.filter(
            pl.struct(["key_marca", "key_modelo"]).map_elements(
                lambda s: wc_match(s["key_marca"], k_marca) and wc_match(s["key_modelo"], k_modelo),
                return_dtype=pl.Boolean
            )
        )

    def _filter_solo_marca_wc(self, k_marca: str) -> pl.DataFrame:
        """
        Busca en self.db_solo_marca (mi_modelo == "*"),
        pero permitiendo comodines en key_marca.
        """
        return self.db_solo_marca.filter(
            pl.col("key_marca").map_elements(
                lambda p: wc_match(p, k_marca),
                return_dtype=pl.Boolean
            )
        )

    def _filter_solo_modelo_wc(self, k_modelo: str) -> pl.DataFrame:
        """
        Busca en self.db_solo_modelo (mi_marca == "*"),
        pero permitiendo comodines en key_modelo.
        """
        return self.db_solo_modelo.filter(
            pl.col("key_modelo").map_elements(
                lambda p: wc_match(p, k_modelo),
                return_dtype=pl.Boolean
            )
        )

def normalizar_rows(df_in: pl.DataFrame, df_db: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    req_min = ["UserProductID", "TITULO", "SKU", "AÑO"]
    missing_rows = []
    ok_source_rows = []

    for row in df_in.to_dicts():
        if any(is_blank_like(row.get(c)) for c in req_min):
            missing_rows.append(row)
        else:
            ok_source_rows.append(row)

    norm = Normalizador(df_db)
    out_ok_rows: list[dict] = []
    for r in ok_source_rows:
        out_ok_rows.extend(norm.transformar(r))

    if out_ok_rows:
        df_ok = pl.DataFrame(out_ok_rows)
        for c in REQ_COLS_IN:
            if c not in df_ok.columns:
                df_ok = df_ok.with_columns(pl.lit("").alias(c).cast(pl.Utf8))
        df_ok = df_ok.select(REQ_COLS_IN)
    else:
        df_ok = pl.DataFrame(schema={c: pl.Utf8 for c in REQ_COLS_IN})

    if missing_rows:
        df_err = pl.DataFrame(missing_rows)
        for c in REQ_COLS_IN:
            if c not in df_err.columns:
                df_err = df_err.with_columns(pl.lit("").alias(c).cast(pl.Utf8))
        df_err = df_err.select(REQ_COLS_IN)
    else:
        df_err = pl.DataFrame(schema={c: pl.Utf8 for c in REQ_COLS_IN})

    return df_ok, df_err


# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Normaliza FABRICANTE/MODELO/AÑO con soporte de CARROCERÍA y TRACCIÓN (BD de normalización).")
    ap.add_argument("--input", required=True, help="Archivo de entrada (XLSX).")
    ap.add_argument("--db", required=True, help="Normalizador (XLSX) con columnas mi_*/ml_*.")
    ap.add_argument("--out-ok", required=True, help="Salida OK (XLSX).")
    ap.add_argument("--out-err", required=True, help="Salida errores (XLSX).")
    ap.add_argument("--csv-encoding", default="utf8", help="(No aplica a XLSX, legado).")
    args = ap.parse_args()

    path_in = Path(args.input)
    path_db = Path(args.db)

    # Leer entrada y DB
    df_in = read_table(path_in, REQ_COLS_IN, csv_encoding=args.csv_encoding)
    df_db = read_table(path_db, REQ_COLS_DB, csv_encoding=args.csv_encoding)

    df_ok, df_err = normalizar_rows(df_in, df_db)

    write_xlsx(Path(args.out_ok), df_ok)
    if df_err.height > 0:
        write_xlsx(Path(args.out_err), df_err)

    print(f"[OK] Filas OK: {df_ok.height} | Errores: {df_err.height}")
    print(f" -> {args.out_ok}")
    if df_err.height > 0:
        print(f" -> {args.out_err}")


if __name__ == "__main__":
    main()
