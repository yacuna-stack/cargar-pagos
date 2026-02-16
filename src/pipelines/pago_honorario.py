"""
pago_honorario.py — Procesa pagos de honorarios (archivos con patrón '<DNI> h')
y deja el resultado visible en "Informacion imagenes" columna Q.
"""

import logging
from typing import Dict, Any

from src.utils.sheets_io import SheetsIO
from src.utils.config import HEADERS_MES
from src.utils.parsers import (
    extraer_dni_honorario,
    parsear_fecha_flexible,
    nombre_hoja_mes,
    parsear_dia_mes_texto,
)
from src.utils.text import extraer_solo_numeros_crudos, limpiar_monto_sin_decimales

logger = logging.getLogger(__name__)

COLS_OUT = len(HEADERS_MES)


def _mes_anterior(anio: int, mes_idx: int) -> Dict[str, int]:
    """mes_idx es 0-based (0=Enero)."""
    if mes_idx <= 0:
        return {"anio": anio - 1, "mesIdx": 11}
    return {"anio": anio, "mesIdx": mes_idx - 1}


def ejecutar_honorarios(sheets: SheetsIO) -> Dict[str, Any]:
    """
    Procesa honorarios:
    1) Filtra filas con patrón '<DNI> h' en filename
    2) Determina mes/año por fecha (col idx 3 de Informacion imagenes)
    3) Busca registro base del DNI en hoja del mes (o mes anterior)
    4) Crea nueva fila (append) con monto del comprobante
    5) Escribe estado por fila en Informacion imagenes, columna Q
    """
    info_data = sheets.leer_info_imagenes()
    if not info_data:
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    # idx (0-based sobre info_data) -> texto en columna Q
    marcas_q: Dict[int, str] = {}

    # Agrupar por mes: "anio-mesIdx" -> items
    grupos: Dict[str, list] = {}

    # ------------- 1) Detectar honorarios en Informacion imagenes -------------
    for i, row in enumerate(info_data):
        archivo = str(row[0] if len(row) > 0 else "").strip()
        dni = extraer_dni_honorario(archivo)
        if not dni:
            continue

        fecha_raw = row[3] if len(row) > 3 else None
        fecha = parsear_fecha_flexible(fecha_raw)
        if not fecha:
            marcas_q[i] = "❌ HON FECHA_INVALIDA"
            continue

        monto_raw = row[5] if len(row) > 5 else ""
        if not str(monto_raw).strip():
            marcas_q[i] = "❌ HON MONTO_VACIO"
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

    # Si no hay honorarios válidos, igual escribimos marcas (fecha/monto inválidos)
    if not grupos:
        if marcas_q:
            sheets.escribir_estado_info_imagenes_col_q(marcas_q)  # columna Q
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    procesados = duplicados = sin_base = 0

    # ------------- 2) Procesar por hoja mensual -------------
    for key, items in grupos.items():
        anio, mes_idx = [int(x) for x in key.split("-")]
        hoja_nombre = nombre_hoja_mes(mes_idx, anio)

        data_mes = sheets.leer_hoja_mes(hoja_nombre) or []

        last_row_by_dni: Dict[str, int] = {}
        honorario_keys = set()

        # recorrer desde abajo para quedarnos con "última fila" por DNI
        for r in range(len(data_mes) - 1, 0, -1):  # salta header en 0
            row_vals = data_mes[r]
            if not row_vals:
                continue

            dni_val = str(row_vals[0] if len(row_vals) > 0 else "").strip()
            if not dni_val:
                continue

            if dni_val not in last_row_by_dni:
                last_row_by_dni[dni_val] = r

            # ✅ Detectar honorarios existentes SOLO por flag (col Y => idx 24)
            flag_h = (len(row_vals) > 24 and row_vals[24] is True)
            if not flag_h:
                continue

            # Para dedupe necesitamos fecha (col C => idx 2) y monto (col V => idx 21)
            if len(row_vals) <= 21:
                continue

            fecha_val = row_vals[2] if len(row_vals) > 2 else ""
            parsed = parsear_dia_mes_texto(str(fecha_val)) if isinstance(fecha_val, str) else None
            if not parsed:
                continue

            monto_sin_exist = limpiar_monto_sin_decimales(row_vals[21])
            honorario_keys.add(f"{dni_val}|{parsed['dia']}|{parsed['mesIdx']}|{monto_sin_exist}")

        # Mes anterior (lazy)
        prev_data = None
        prev_by_dni = None

        appends = []

        for it in items:
            monto_sin = limpiar_monto_sin_decimales(it["monto_raw"])
            dup_key = f"{it['dni']}|{it['dia']}|{it['mesIdx']}|{monto_sin}"

            # Duplicado
            if dup_key in honorario_keys:
                duplicados += 1
                marcas_q[it["idx"]] = "⚠️ HON DUP"
                continue

            # Buscar base en mes actual
            base_row = None
            if it["dni"] in last_row_by_dni:
                base_row = list(data_mes[last_row_by_dni[it["dni"]]])
            else:
                # Fallback: mes anterior
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
                else:
                    sin_base += 1
                    marcas_q[it["idx"]] = "❌ HON SIN_BASE"
                    continue

            # Asegurar longitud exacta
            while len(base_row) < COLS_OUT:
                base_row.append("")
            base_row = base_row[:COLS_OUT]

            # Modificar para honorario
            importe_fmt = extraer_solo_numeros_crudos(it["monto_raw"])

            base_row[3] = importe_fmt   # D: Importe
            base_row[21] = monto_sin    # V: MONTPAGO
            base_row[23] = monto_sin    # X: MANGO
            base_row[15] = ""           # P: Limpiar estado
            base_row[24] = True         # Y: Honorario
            base_row[13] = "Banco"      # N: Cta. Destino
            base_row[22] = 2            # W: Código Banco

            appends.append(base_row)
            honorario_keys.add(dup_key)
            procesados += 1
            marcas_q[it["idx"]] = f"✅ HON OK → {hoja_nombre}"

        if appends:
            sheets.escribir_filas_mes(hoja_nombre, appends)

    # ------------- 3) Escribir estados en Informacion imagenes, columna Q -------------
    if marcas_q:
        sheets.escribir_estado_info_imagenes_col_q(marcas_q)

    logger.info(f"Honorarios: {procesados} OK, {duplicados} dup, {sin_base} sin base")
    return {"procesados": procesados, "duplicados": duplicados, "sin_base": sin_base}
