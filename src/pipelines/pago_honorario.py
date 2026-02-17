"""
pago_honorario.py — Procesa pagos de honorarios (archivos con patrón '<DNI> h')
Deja el resultado visible en "Informacion imagenes" columna Q.
"""

import logging
from typing import Dict, Any, List

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


def _mes_anterior(anio: int, mes_idx: int) -> Dict[str, int]:
    if mes_idx <= 0:
        return {"anio": anio - 1, "mesIdx": 11}
    return {"anio": anio, "mesIdx": mes_idx - 1}


def ejecutar_honorarios(sheets: SheetsIO) -> Dict[str, Any]:

    info_data = sheets.leer_info_imagenes()
    if not info_data:
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    marcas_q: Dict[int, str] = {}
    grupos: Dict[str, List[dict]] = {}

    # =========================================================
    # 1️⃣ PRE-FILTRADO Y AGRUPACIÓN (O(n))
    # =========================================================
    for i, row in enumerate(info_data):

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
    # 2️⃣ PROCESAMIENTO POR MES
    # =========================================================
    for key, items in grupos.items():

        anio, mes_idx = [int(x) for x in key.split("-")]
        hoja_nombre = nombre_hoja_mes(mes_idx, anio)

        data_mes = sheets.leer_hoja_mes(hoja_nombre) or []

        # Índices rápidos
        last_row_by_dni: Dict[str, int] = {}
        honorario_keys = set()

        # =========================================
        # CONSTRUIR ÍNDICES EXISTENTES (O(n))
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

            # Solo si flag Y=True
            flag_h = (len(row_vals) > 24 and row_vals[24] is True)
            if not flag_h:
                continue

            if len(row_vals) <= 21:
                continue

            fecha_val = row_vals[2]
            parsed = parsear_dia_mes_texto(str(fecha_val))
            if not parsed:
                continue

            monto_sin = limpiar_monto_sin_decimales(row_vals[21])
            honorario_keys.add(
                f"{dni_val}|{parsed['dia']}|{parsed['mesIdx']}|{monto_sin}"
            )

        # =========================================
        # Fallback mes anterior (lazy load)
        # =========================================
        prev_data = None
        prev_by_dni = None

        appends = []

        for it in items:

            monto_sin = limpiar_monto_sin_decimales(it["monto_raw"])
            dup_key = f"{it['dni']}|{it['dia']}|{it['mesIdx']}|{monto_sin}"

            # 🛑 1) DEDUPE FUERTE
            if dup_key in honorario_keys:
                duplicados += 1
                marcas_q[it["idx"]] = f"⚠️ HON DUP: Ya existe en {hoja_nombre}"
                continue

            # 🧠 2) BUSCAR BASE
            base_row = None

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
                else:
                    sin_base += 1
                    marcas_q[it["idx"]] = "❌ HON ERROR: Sin registro base"
                    continue

            # =========================================
            # PREPARAR FILA NUEVA
            # =========================================
            while len(base_row) < COLS_OUT:
                base_row.append("")
            base_row = base_row[:COLS_OUT]

            importe_fmt = extraer_solo_numeros_crudos(it["monto_raw"])

            base_row[3] = importe_fmt         # D
            base_row[21] = monto_sin          # V
            base_row[23] = ""                 # ❗ X queda VACÍA
            base_row[15] = ""                 # limpiar estado
            base_row[24] = True               # Y Honorario
            base_row[13] = "Banco"            # N
            base_row[22] = 2                  # W

            appends.append(base_row)
            honorario_keys.add(dup_key)
            procesados += 1

            marcas_q[it["idx"]] = f"✅ HON OK → {hoja_nombre}"

        # Escritura batch única (más rápido)
        if appends:
            sheets.escribir_filas_mes(hoja_nombre, appends)

    # =========================================================
    # 3️⃣ ESCRIBIR RESULTADO EN COLUMNA Q
    # =========================================================
    if marcas_q:
        sheets.escribir_estado_info_imagenes_col_q(marcas_q)

    logger.info(
        f"Honorarios finalizado: OK={procesados}, DUP={duplicados}, SIN_BASE={sin_base}"
    )

    return {
        "procesados": procesados,
        "duplicados": duplicados,
        "sin_base": sin_base,
    }
