# -*- coding: utf-8 -*-
import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl

from generar_agregar_y_clonar import generar_agregar_clonar_sin
from normalizador_v2 import REQ_COLS_DB, normalizar_rows, read_table


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        ln = line.strip()
        if not ln or ln.startswith("#") or "=" not in ln:
            continue
        key, val = ln.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_pd_xlsx(path: Path, df: pd.DataFrame) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.fillna("").to_excel(w, index=False)


def write_chunked_pd(df: pd.DataFrame, out_dir: Path, prefix: str, ts: str, chunk_size: int) -> list[Path]:
    files: list[Path] = []
    if df.empty:
        return files
    total = len(df)
    chunks = max(1, (total + chunk_size - 1) // chunk_size)
    for i in range(chunks):
        start = i * chunk_size
        end = min(start + chunk_size, total)
        path = out_dir / f"{prefix}_{ts}_{i+1}.xlsx"
        write_pd_xlsx(path, df.iloc[start:end].copy())
        files.append(path)
    return files


def write_chunked_pl(df: pl.DataFrame, out_dir: Path, prefix: str, ts: str, chunk_size: int) -> list[Path]:
    files: list[Path] = []
    if df.height == 0:
        return files
    chunks = max(1, (df.height + chunk_size - 1) // chunk_size)
    for i in range(chunks):
        start = i * chunk_size
        path = out_dir / f"{prefix}_{ts}_{i+1}.xlsx"
        pdf = df.slice(start, chunk_size).with_columns(pl.all().cast(pl.Utf8)).to_pandas().fillna("")
        write_pd_xlsx(path, pdf)
        files.append(path)
    return files


def main() -> None:
    load_env_file(Path(".env"))

    ap = argparse.ArgumentParser(description="Pipeline único: Generar Agregar/Clonar/Sin + Normalizar + Exportar chunked.")
    ap.add_argument("--input", required=True, help="Archivo de publicaciones (XLSX).")
    ap.add_argument("--db", default=os.getenv("COMPAT_DB_PATH", "compat_db.sqlite"), help="Ruta de compat_db.sqlite.")
    ap.add_argument("--normalizador-db", default=os.getenv("NORMALIZADOR_DB_PATH", "normalizador.xlsx"), help="Ruta de normalizador.xlsx.")
    ap.add_argument("--pos-normalizer", default=os.getenv("POS_NORMALIZER_PATH", "normalizador_posiciones.xlsx"), help="Ruta de normalizador_posiciones.xlsx.")
    ap.add_argument("--carroceria-normalizer", default=os.getenv("CARROCERIA_NORMALIZER_PATH", "normalizador_carroceria.xlsx"), help="Ruta de normalizador_carroceria.xlsx.")
    ap.add_argument("--traccion-normalizer", default=os.getenv("TRACCION_NORMALIZER_PATH", "normalizador_traccion.xlsx"), help="Ruta de normalizador_traccion.xlsx.")
    ap.add_argument("--output-dir", default=os.getenv("OUTPUT_BASE_DIR", "."), help="Directorio base de salida.")
    ap.add_argument("--chunk-agregar", type=int, default=int(os.getenv("CHUNK_AGREGAR", "50000")), help="Filas por archivo de AGREGAR.")
    ap.add_argument("--chunk-clonar", type=int, default=int(os.getenv("CHUNK_CLONAR", "7000")), help="Filas por archivo de CLONAR.")
    args = ap.parse_args()

    if args.chunk_agregar <= 0 or args.chunk_clonar <= 0:
        raise SystemExit("chunk-agregar y chunk-clonar deben ser mayores a 0.")

    out_base = Path(args.output_dir)
    dir_agregar = ensure_dir(out_base / "AGREGAR_COMPATIBILIDADES")
    dir_clonar = ensure_dir(out_base / "CLONAR_COMPATIBILIDADES")
    dir_sin = ensure_dir(out_base / "SIN_DATOS")
    dir_err = ensure_dir(out_base / "ERRORES")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    df_agregar_raw, df_clonar, df_sin, total_input = generar_agregar_clonar_sin(
        input_path=Path(args.input),
        db_path=Path(args.db),
        pos_normalizer_path=Path(args.pos_normalizer),
        carroceria_normalizer_path=Path(args.carroceria_normalizer),
        traccion_normalizer_path=Path(args.traccion_normalizer),
    )

    df_db = read_table(Path(args.normalizador_db), REQ_COLS_DB)
    df_agregar_in = pl.from_pandas(df_agregar_raw.fillna("").astype(str)) if not df_agregar_raw.empty else pl.DataFrame(
        schema={c: pl.Utf8 for c in df_agregar_raw.columns}
    )
    df_ok, df_err = normalizar_rows(df_agregar_in, df_db)

    files_agregar = write_chunked_pl(df_ok, dir_agregar, "agrega_compatibilidad", ts, args.chunk_agregar)
    files_clonar = write_chunked_pd(df_clonar, dir_clonar, "clonar_compatibilidades", ts, args.chunk_clonar)

    sin_file = None
    if not df_sin.empty:
        sin_file = dir_sin / f"sin_datos_{ts}.xlsx"
        write_pd_xlsx(sin_file, df_sin)

    err_file = None
    if df_err.height > 0:
        err_file = dir_err / f"errores_{ts}.xlsx"
        write_pd_xlsx(err_file, df_err.to_pandas().fillna(""))

    print(f"[OK] Filas input: {total_input}")
    print(f"[OK] Sin datos: {len(df_sin)}")
    print(f"[OK] Clonar: {len(df_clonar)}")
    print(f"[OK] Agregar pre-normalizar: {len(df_agregar_raw)}")
    print(f"[OK] Agregar normalizado (out-ok): {df_ok.height}")
    print(f"[OK] Errores normalización (out-err): {df_err.height}")
    print(f"[OK] Archivos AGREGAR_COMPATIBILIDADES: {len(files_agregar)}")
    print(f"[OK] Archivos CLONAR_COMPATIBILIDADES: {len(files_clonar)}")
    print(f"[OK] Archivo SIN_DATOS: {1 if sin_file else 0}")
    print(f"[OK] Archivo ERRORES: {1 if err_file else 0}")


if __name__ == "__main__":
    main()
