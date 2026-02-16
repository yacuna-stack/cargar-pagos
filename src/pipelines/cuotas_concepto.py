"""
cuotas_concepto.py — Recalcula Concepto (E) y Nro Cuota (G) con acumulado real.
Reemplaza el scheduler actualizarCuotasYConceptos de Apps Script.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List

from src.utils.sheets_io import SheetsIO
from src.utils.config import MESES_ES, MESES_ABREV
from src.utils.text import normalize, parsear_monto_float
from src.utils.parsers import nombre_hoja_mes

logger = logging.getLogger(__name__)


def _unique_key(dni: str, cartera: str) -> str:
    return f"{normalize(dni)}_{normalize(cartera)}"


def _parse_mes_anio_header(header: str):
    """Parsea 'Enero 26' o 'pago Enero 26' → (mesIdx, anio)."""
    h = normalize(header)
    for i, mes in enumerate(MESES_ES):
        mes_norm = normalize(mes)
        if mes_norm in h:
            # Buscar año
            import re
            m = re.search(r"(\d{2,4})", header)
            if m:
                anio = int(m.group(1))
                if anio < 100:
                    anio += 2000
                return i, anio
    return None, None


def _get_pago_columns(header_pro: List[str]) -> List[Dict]:
    """Encuentra columnas 'pago <mes> <año>' en Pro con su orden cronológico."""
    pagos = []
    for k, h in enumerate(header_pro):
        h_lower = str(h or "").lower()
        if "pago" not in h_lower:
            continue
        mes_idx, anio = _parse_mes_anio_header(h)
        if mes_idx is not None and anio is not None:
            pagos.append({"col": k, "mesIdx": mes_idx, "anio": anio, "key": anio * 12 + mes_idx})
    pagos.sort(key=lambda x: x["key"])
    return pagos


def _sumar_historia(row_pro: List, pago_cols: List[Dict], key_mes_actual: int) -> float:
    """Suma pagos de meses anteriores al actual."""
    total = 0.0
    for p in pago_cols:
        if p["key"] >= key_mes_actual:
            break
        val = parsear_monto_float(row_pro[p["col"]] if p["col"] < len(row_pro) else "")
        total += val
    return total


def _find_valor_cuota_col(header_pro: List[str], mes_idx: int, anio: int) -> int:
    """Encuentra columna de valor cuota del mes (no 'pago', no 'saldo')."""
    for k, h in enumerate(header_pro):
        h_lower = str(h or "").lower()
        if "pago" in h_lower or "saldo" in h_lower or "cobra" in h_lower:
            continue
        mi, a = _parse_mes_anio_header(h)
        if mi == mes_idx and a == anio:
            return k
    return -1


def ejecutar_cuotas_concepto(sheets: SheetsIO) -> Dict[str, Any]:
    """
    Recalcula Concepto y Nro Cuota para la hoja del mes actual.
    Usa acumulado histórico real (suma meses previos + mes en curso fila a fila).
    """
    now = datetime.now()
    mes_idx = now.month - 1
    anio = now.year
    hoja_nombre = nombre_hoja_mes(mes_idx, anio)
    key_mes_actual = anio * 12 + mes_idx

    # Leer Pro
    header_pro, data_pro = sheets.leer_pro()
    if not data_pro:
        return {"actualizadas": 0}

    # Mapa DNI+Cartera → idx en Pro
    mapa = {}
    for i, row in enumerate(data_pro):
        dni = str(row[2] if len(row) > 2 else "").strip()
        cartera = str(row[9] if len(row) > 9 else "").strip()
        if dni:
            mapa[_unique_key(dni, cartera)] = i

    # Columnas de pago en Pro
    pago_cols = _get_pago_columns(header_pro)
    col_valor = _find_valor_cuota_col(header_pro, mes_idx, anio)

    if col_valor == -1:
        logger.warning(f"No se encontró columna de valor cuota para {MESES_ES[mes_idx]} {anio}")
        return {"actualizadas": 0}

    # Leer hoja del mes
    data_mes = sheets.leer_hoja_mes(hoja_nombre)
    if len(data_mes) <= 1:
        return {"actualizadas": 0}

    total_rows = len(data_mes) - 1  # sin header
    conceptos = []
    cuotas = []
    acumulador = {}  # {key: monto_acumulado_mes}
    actualizadas = 0

    for i in range(1, len(data_mes)):
        row = data_mes[i]
        dni = str(row[0] if len(row) > 0 else "").strip()
        cartera = str(row[8] if len(row) > 8 else "").strip()
        key = _unique_key(dni, cartera)

        pago_fila = parsear_monto_float(row[3] if len(row) > 3 else "")

        concepto = ""
        cuota = ""

        if dni and key in mapa:
            idx_pro = mapa[key]
            row_pro = data_pro[idx_pro]

            cant_cuotas = 999
            try:
                cant_cuotas = int(row_pro[7]) if len(row_pro) > 7 else 999
            except (ValueError, TypeError):
                pass
            if cant_cuotas < 1:
                cant_cuotas = 999

            val_cuota = parsear_monto_float(row_pro[col_valor] if col_valor < len(row_pro) else "")

            if val_cuota > 0:
                historia = _sumar_historia(row_pro, pago_cols, key_mes_actual)
                acum_previo = acumulador.get(key, 0.0)
                estado = historia + acum_previo + pago_fila

                acumulador[key] = acum_previo + pago_fila

                monto_total_contrato = val_cuota * cant_cuotas

                # ─── Lógica de decisión ───
                if estado >= (monto_total_contrato - 50):
                    concepto = "Total"
                    cuota = cant_cuotas
                elif estado < (val_cuota - 50):
                    concepto = "Parcial"
                    cuota = 1
                else:
                    ratio = estado / val_cuota
                    resto = estado % val_cuota
                    es_exacto = resto < 100 or (val_cuota - resto) < 100

                    if es_exacto:
                        concepto = "Cuota"
                        cuota = round(ratio)
                    else:
                        concepto = "Parcial"
                        cuota = int(ratio) + 1

                    if cuota > cant_cuotas:
                        cuota = cant_cuotas

                actualizadas += 1
            else:
                concepto = "Falta Valor"
        else:
            concepto = "No Match" if dni else ""

        conceptos.append(concepto)
        cuotas.append(cuota)

    # Escribir columnas E y G
    sheets.actualizar_dos_columnas_mes(hoja_nombre, conceptos, cuotas)

    logger.info(f"Cuotas: {actualizadas} filas actualizadas en '{hoja_nombre}'")
    return {"actualizadas": actualizadas}