"""
pago_honorario.py — Procesa pagos de honorarios (archivos con patrón '<DNI> h')
Deja el resultado visible en "Informacion imagenes" columna Q.
"""

import logging
from typing import Dict, Any, List, Optional

from src.utils.sheets_io import SheetsIO
from src.utils.config import HEADERS_MES
from src.utils.parsers import (
    extraer_dni_honorario,
    parsear_fecha_flexible,
    nombre_hoja_mes,
    parsear_dia_mes_texto,
)
from src.utils.text import (
    extraer_solo_numeros_crudos,
    limpiar_monto_sin_decimales,
)

logger = logging.getLogger(__name__)

COLS_OUT = len(HEADERS_MES)

# Informacion imagenes: Q es columna 17 (1-based), en get_all_values está en índice 16 (0-based)
INFO_Q_IDX = 16


def _mes_anterior(anio: int, mes_idx: int) -> Dict[str, int]:
    if mes_idx <= 0:
        return {"anio": anio - 1, "mesIdx": 11}
    return {"anio": anio, "mesIdx": mes_idx - 1}


def _is_truthy(v: Any) -> bool:
    """Robusto para valores que llegan como bool, string, número, etc."""
    if v is True:
        return True
    if v is False or v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in ("true", "verdadero", "1", "yes", "y", "si", "sí")


def _es_honorario_existente(row_vals: List[Any]) -> bool:
    """
    Replica robusta del criterio del Apps Script:
    es honorario si (Y === true) OR (col J tiene valor)
    - Y (idx 24)
    - J (idx 9)
    """
    y_val = row_vals[24] if len(row_vals) > 24 else None
    j_val = str(row_vals[9]).strip() if len(row_vals) > 9 and row_vals[9] is not None else ""
    return _is_truthy(y_val) or (j_val != "")


def _build_dedupe_key(dni: str, dia: int, mes_idx: int, monto_raw: Any) -> str:
    monto_sin = limpiar_monto_sin_decimales(monto_raw)
    return f"{dni}|{dia}|{mes_idx}|{monto_sin}"


def ejecutar_honorarios(sheets: SheetsIO) -> Dict[str, Any]:

    info_data = sheets.leer_info_imagenes()
    if not info_data:
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    marcas_q: Dict[int, str] = {}
    grupos: Dict[str, List[dict]] = {}

    # =========================================================
    # 1) PRE-FILTRADO Y AGRUPACIÓN
    # =========================================================
    for i, row in enumerate(info_data):

        # (Opcional pero recomendado) si ya fue procesado, no lo reprocesamos
        q_actual = str(row[INFO_Q_IDX]).strip() if len(row) > INFO_Q_IDX and row[INFO_Q_IDX] is not None else ""
        if q_actual.startswith("✅ HON OK") or q_actual.startswith("⚠️ HON DUP"):
            # Ya resuelto en ejecuciones anteriores
            continue

        archivo = str(row[0] if len(row) > 0 else "").strip()
        dni = extraer_dni_honorario(archivo)
        if not dni:
            continue

        fecha_raw = row[3] if len(row) > 3 else None
        fecha = parsear_fecha_flexible(fecha_raw)
        if not fecha:
            marcas_q[i] = "❌ HON ERROR: Fecha inválida"
            continue

        monto_raw = row[5] if len(row) > 5 else ""
        if not str(monto_raw).strip():
            marcas_q[i] = "❌ HON ERROR: Monto vacío"
            continue

        key = f"{fecha['anio']}-{fecha['mesIdx']}"
        grupos.setdefault(key, []).append({
            "idx": i,
            "dni": dni,
            "monto_raw": monto_raw,
            "dia": fecha["dia"],
            "mesIdx": fecha["mesIdx"],
            "anio": fecha["anio"],
        })

    if not grupos:
        if marcas_q:
            sheets.escribir_estado_info_imagenes_col_q(marcas_q)
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    procesados = duplicados = sin_base = 0

    # =========================================================
    # 2) PROCESAMIENTO POR MES
    # =========================================================
    for key, items in grupos.items():

        anio, mes_idx = [int(x) for x in key.split("-")]
        hoja_nombre = nombre_hoja_mes(mes_idx, anio)

        data_mes = sheets.leer_hoja_mes(hoja_nombre) or []

        last_row_by_dni: Dict[str, int] = {}
        honorario_keys = set()

        # =========================================
        # CONSTRUIR ÍNDICES EXISTENTES
        # =========================================
        for r in range(len(data_mes) - 1, 0, -1):
            row_vals = data_mes[r]
            if not row_vals:
                continue

            dni_val = str(row_vals[0] if len(row_vals) > 0 else "").strip()
            if not dni_val:
                continue

            if dni_val not in last_row_by_dni:
                last_row_by_dni[dni_val] = r

            # ✅ criterio robusto (Y truthy OR J con valor)
            if not _es_honorario_existente(row_vals):
                continue

            # Para dedupe: fecha (C idx 2) y monto (V idx 21)
            if len(row_vals) <= 21:
                continue

            fecha_val = row_vals[2] if len(row_vals) > 2 else ""
            parsed = parsear_dia_mes_texto(str(fecha_val))
            if not parsed:
                continue

            dedupe_key = _build_dedupe_key(
                dni=dni_val,
                dia=parsed["dia"],
                mes_idx=parsed["mesIdx"],
                monto_raw=row_vals[21],
            )
            honorario_keys.add(dedupe_key)

        # =========================================
        # Fallback mes anterior (lazy load)
        # =========================================
        prev_data = None
        prev_by_dni = None
        prev_nombre = None

        appends = []

        for it in items:

            dup_key = _build_dedupe_key(
                dni=it["dni"],
                dia=it["dia"],
                mes_idx=it["mesIdx"],
                monto_raw=it["monto_raw"],
            )

            # 🛑 DEDUPE REAL
            if dup_key in honorario_keys:
                duplicados += 1
                marcas_q[it["idx"]] = f"⚠️ HON DUP: Ya existe (no se carga) en {hoja_nombre}"
                continue

            # 🧠 BUSCAR BASE
            base_row = None
            base_from = hoja_nombre

            if it["dni"] in last_row_by_dni:
                base_row = list(data_mes[last_row_by_dni[it["dni"]]])
            else:
                if prev_data is None:
                    prev = _mes_anterior(anio, mes_idx)
                    prev_nombre = nombre_hoja_mes(prev["mesIdx"], prev["anio"])
                    prev_data = sheets.leer_hoja_mes(prev_nombre) or []
                    prev_by_dni = {}
                    for r in range(len(prev_data) - 1, 0, -1):
                        d = str(prev_data[r][0] if len(prev_data[r]) > 0 else "").strip()
                        if d and d not in prev_by_dni:
                            prev_by_dni[d] = r

                if prev_by_dni and it["dni"] in prev_by_dni:
                    base_row = list(prev_data[prev_by_dni[it["dni"]]])
                    base_from = prev_nombre or "mes anterior"
                else:
                    sin_base += 1
                    marcas_q[it["idx"]] = "❌ HON ERROR: Sin registro base (mes actual ni anterior)"
                    continue

            # Asegurar longitud exacta
            while len(base_row) < COLS_OUT:
                base_row.append("")
            base_row = base_row[:COLS_OUT]

            # Modificar para honorario
            importe_fmt = extraer_solo_numeros_crudos(it["monto_raw"])
            monto_sin = limpiar_monto_sin_decimales(it["monto_raw"])

            base_row[3] = importe_fmt   # D Importe
            base_row[21] = monto_sin    # V MONTPAGO
            base_row[23] = ""           # X VACÍA (regla tuya)
            base_row[15] = ""           # P limpiar estado
            base_row[24] = True         # Y honorario
            base_row[13] = "Banco"      # N
            base_row[22] = 2            # W

            appends.append(base_row)

            # importantísimo: sumar al set ANTES de seguir, para no duplicar en el mismo run
            honorario_keys.add(dup_key)
            procesados += 1

            if base_from != hoja_nombre:
                marcas_q[it["idx"]] = f"✅ HON OK → {hoja_nombre} (base: {base_from})"
            else:
                marcas_q[it["idx"]] = f"✅ HON OK → {hoja_nombre}"

        if appends:
            sheets.escribir_filas_mes(hoja_nombre, appends)

    # =========================================================
    # 3) ESCRIBIR ESTADOS EN Q
    # =========================================================
    if marcas_q:
        sheets.escribir_estado_info_imagenes_col_q(marcas_q)

    logger.info(f"Honorarios finalizado: OK={procesados}, DUP={duplicados}, SIN_BASE={sin_base}")
    return {"procesados": procesados, "duplicados": duplicados, "sin_base": sin_base}
