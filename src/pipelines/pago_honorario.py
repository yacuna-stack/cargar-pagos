"""
pago_honorario.py — Procesa pagos de honorarios (archivos con patrón '<DNI> h')
"""

import logging
from typing import Dict, Any, List

from src.utils.sheets_io import SheetsIO
from src.utils.config import MESES_ES, MESES_ABREV, HEADERS_MES
from src.utils.parsers import (
    extraer_dni_honorario,
    parsear_fecha_flexible,
    nombre_hoja_mes,
    formato_fecha_corta,
)
from src.utils.text import extraer_solo_numeros_crudos, limpiar_monto_sin_decimales

logger = logging.getLogger(__name__)

COLS_OUT = len(HEADERS_MES)


def _mes_anterior(anio: int, mes_idx: int) -> Dict:
    if mes_idx <= 0:
        return {"anio": anio - 1, "mesIdx": 11}
    return {"anio": anio, "mesIdx": mes_idx - 1}


def ejecutar_honorarios(sheets: SheetsIO) -> Dict[str, Any]:
    """
    Procesa honorarios:
    1. Filtra filas con patrón '<DNI> h' en filename
    2. Busca registro base del DNI en hoja del mes (o mes anterior)
    3. Crea nueva fila con monto del comprobante
    """
    info_data = sheets.leer_info_imagenes()
    if not info_data:
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    # Filtrar y agrupar por mes
    grupos = {}  # {"anio-mesIdx": [items]}

    for i, row in enumerate(info_data):
        archivo = str(row[0] if len(row) > 0 else "").strip()
        dni = extraer_dni_honorario(archivo)
        if not dni:
            continue

        fecha_raw = row[3] if len(row) > 3 else None
        fecha = parsear_fecha_flexible(fecha_raw)
        if not fecha:
            continue

        monto_raw = row[5] if len(row) > 5 else ""
        key = f"{fecha['anio']}-{fecha['mesIdx']}"

        if key not in grupos:
            grupos[key] = []

        grupos[key].append({
            "idx": i,
            "dni": dni,
            "monto_raw": monto_raw,
            "dia": fecha["dia"],
            "mesIdx": fecha["mesIdx"],
            "anio": fecha["anio"],
        })

    if not grupos:
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    procesados = duplicados = sin_base = 0

    for key, items in grupos.items():
        anio, mes_idx = [int(x) for x in key.split("-")]
        hoja_nombre = nombre_hoja_mes(mes_idx, anio)

        # Leer datos del mes
        data_mes = sheets.leer_hoja_mes(hoja_nombre)

        # Índice por DNI (última fila)
        last_row_by_dni = {}
        honorario_keys = set()

        for r in range(len(data_mes) - 1, 0, -1):
            row_vals = data_mes[r]
            if len(row_vals) == 0:
                continue

            dni_val = str(row_vals[0]).strip()
            if not dni_val:
                continue

            if dni_val not in last_row_by_dni:
                last_row_by_dni[dni_val] = r

            # Detectar honorarios existentes
            is_honorario = (len(row_vals) > 24 and row_vals[24] is True) or \
                           (len(row_vals) > 9 and str(row_vals[9]).strip())

            if is_honorario and len(row_vals) > 21:
                from src.utils.parsers import parsear_dia_mes_texto
                fecha_val = row_vals[2] if len(row_vals) > 2 else ""
                parsed = parsear_dia_mes_texto(str(fecha_val)) if isinstance(fecha_val, str) else None
                if parsed:
                    monto_sin = limpiar_monto_sin_decimales(row_vals[21])
                    honorario_keys.add(f"{dni_val}|{parsed['dia']}|{parsed['mesIdx']}|{monto_sin}")

        # Datos mes anterior (lazy)
        prev_data = None
        prev_by_dni = None

        appends = []

        for it in items:
            monto_sin = limpiar_monto_sin_decimales(it["monto_raw"])
            dup_key = f"{it['dni']}|{it['dia']}|{it['mesIdx']}|{monto_sin}"

            # Duplicado
            if dup_key in honorario_keys:
                duplicados += 1
                continue

            # Buscar base en mes actual
            base_row = None
            if it["dni"] in last_row_by_dni:
                idx = last_row_by_dni[it["dni"]]
                base_row = list(data_mes[idx])
            else:
                # Fallback: mes anterior
                if prev_data is None:
                    prev = _mes_anterior(anio, mes_idx)
                    prev_nombre = nombre_hoja_mes(prev["mesIdx"], prev["anio"])
                    prev_data = sheets.leer_hoja_mes(prev_nombre)
                    prev_by_dni = {}
                    for r in range(len(prev_data) - 1, 0, -1):
                        d = str(prev_data[r][0] if len(prev_data[r]) > 0 else "").strip()
                        if d and d not in prev_by_dni:
                            prev_by_dni[d] = r

                if prev_by_dni and it["dni"] in prev_by_dni:
                    idx = prev_by_dni[it["dni"]]
                    base_row = list(prev_data[idx])
                else:
                    sin_base += 1
                    continue

            # Asegurar longitud
            while len(base_row) < COLS_OUT:
                base_row.append("")
            base_row = base_row[:COLS_OUT]

            # Modificar para honorario
            importe_fmt = extraer_solo_numeros_crudos(it["monto_raw"])

            base_row[3] = importe_fmt       # D: Importe
            base_row[21] = monto_sin        # V: MONTPAGO
            base_row[23] = monto_sin        # X: MANGO
            base_row[15] = ""               # P: Limpiar estado
            base_row[24] = True             # Y: Honorario
            base_row[13] = "Banco"          # N: Cta. Destino
            base_row[22] = 2               # W: Código Banco

            appends.append(base_row)
            honorario_keys.add(dup_key)
            procesados += 1

        # Escribir batch
        if appends:
            sheets.escribir_filas_mes(hoja_nombre, appends)

    logger.info(f"Honorarios: {procesados} OK, {duplicados} dup, {sin_base} sin base")
    return {"procesados": procesados, "duplicados": duplicados, "sin_base": sin_base}