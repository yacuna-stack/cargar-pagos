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
    formato_fecha_corta,
)
from src.utils.text import (
    extraer_solo_numeros_crudos,
    limpiar_monto_sin_decimales,
    es_banco_comafi,
    detectar_entidades,
)
from src.utils.calendar_ar import calcular_dia_habil_del_mes

logger = logging.getLogger(__name__)

COLS_OUT = len(HEADERS_MES)

# "Informacion imagenes": Q es columna 17 (1-based), en get_all_values está en índice 16 (0-based)
INFO_Q_IDX = 16


def _mes_anterior(anio: int, mes_idx: int) -> Dict[str, int]:
    """mes_idx es 0-based (0=Enero)."""
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
    """
    Key de dedupe: DNI + día + mesIdx (0-based) + montoSinDec (estable).
    """
    monto_sin = limpiar_monto_sin_decimales(monto_raw)
    return f"{dni}|{dia}|{mes_idx}|{monto_sin}"


def ejecutar_honorarios(sheets: SheetsIO) -> Dict[str, Any]:
    info_data = sheets.leer_info_imagenes()
    if not info_data:
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    # idx (0-based sobre info_data) -> texto en columna Q
    marcas_q: Dict[int, str] = {}

    # Agrupar por mes: "anio-mesIdx" -> items
    grupos: Dict[str, List[dict]] = {}

    # =========================================================
    # 1) PRE-FILTRADO Y AGRUPACIÓN
    # =========================================================
    for i, row in enumerate(info_data):

        # ⚠️ OJO: si limpias Q siempre, este "skip" deja de servir.
        # Si querés un "skip persistente", usá otra columna/flag permanente.
        #
        # q_actual = str(row[INFO_Q_IDX]).strip() if len(row) > INFO_Q_IDX and row[INFO_Q_IDX] is not None else ""
        # if q_actual.startswith("✅ HON OK") or q_actual.startswith("⚠️ HON DUP"):
        #     continue

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

    # Si no hay honorarios válidos, igual limpiamos Q y escribimos errores detectados
    if not grupos:
        logs_q = [""] * len(info_data)
        for idx, txt in marcas_q.items():
            if 0 <= idx < len(logs_q):
                logs_q[idx] = txt
        sheets.limpiar_y_escribir_info_col_q(logs_q)
        return {"procesados": 0, "duplicados": 0, "sin_base": 0}

    procesados = duplicados = sin_base = 0

    # =========================================================
    # 2) PROCESAMIENTO POR MES
    # =========================================================
    for key, items in grupos.items():
        anio, mes_idx = [int(x) for x in key.split("-")]
        hoja_nombre = nombre_hoja_mes(mes_idx, anio)

        data_mes = sheets.leer_hoja_mes(hoja_nombre) or []

        # Índices rápidos
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
        # Fallback mes anterior y PRO (lazy load)
        # =========================================
        prev_data = None
        prev_by_dni = None
        prev_nombre = None
        
        data_pro = None
        pro_by_dni = None

        appends: List[List[Any]] = []

        for it in items:
            dup_key = _build_dedupe_key(
                dni=it["dni"],
                dia=it["dia"],
                mes_idx=it["mesIdx"],
                monto_raw=it["monto_raw"],
            )

            # 🛑 DEDUPE REAL: si ya existe, NO escribir
            if dup_key in honorario_keys:
                duplicados += 1
                marcas_q[it["idx"]] = f"⚠️ HON DUP: Ya existe (no se carga) en {hoja_nombre}"
                continue

            # 🧠 BUSCAR BASE
            base_row = None
            base_from = hoja_nombre
            
            fecha_fmt = formato_fecha_corta(it["dia"], it["mesIdx"])
            dia_habil = calcular_dia_habil_del_mes(it["dia"], it["mesIdx"], anio) or ""

            if it["dni"] in last_row_by_dni:
                base_row = list(data_mes[last_row_by_dni[it["dni"]]])
            else:
                if prev_data is None:
                    prev = _mes_anterior(anio, mes_idx)
                    prev_nombre = nombre_hoja_mes(prev["mesIdx"], prev["anio"])
                    prev_data = sheets.leer_hoja_mes(prev_nombre) or []
                    prev_by_dni = {}
                    for rr in range(len(prev_data) - 1, 0, -1):
                        d = str(prev_data[rr][0] if len(prev_data[rr]) > 0 else "").strip()
                        if d and d not in prev_by_dni:
                            prev_by_dni[d] = rr

                if prev_by_dni and it["dni"] in prev_by_dni:
                    base_row = list(prev_data[prev_by_dni[it["dni"]]])
                    base_from = prev_nombre or "mes anterior"
                else:
                    if pro_by_dni is None:
                        header_pro, data_pro = sheets.leer_pro()
                        pro_by_dni = {}
                        if data_pro:
                            for idx_pro, r_pro in enumerate(data_pro):
                                d = str(r_pro[2] if len(r_pro) > 2 else "").strip()
                                if d and d not in pro_by_dni:
                                    pro_by_dni[d] = r_pro
                    
                    if pro_by_dni and it["dni"] in pro_by_dni:
                        row_pro = pro_by_dni[it["dni"]]
                        cartera_raw = str(row_pro[9] if len(row_pro) > 9 else "").strip()
                        cartera = "Comafi" if es_banco_comafi(cartera_raw) else cartera_raw
                        
                        base_row = [
                            it["dni"],                                     # A: DNI
                            str(row_pro[3] if len(row_pro) > 3 else ""),   # B: Nombre
                            fecha_fmt,                                     # C: Fecha
                            "",                                            # D: Importe (se pisa)
                            "",                                            # E: Concepto
                            "",                                            # F: Tipo de Pago
                            "",                                            # G: Nro de Cuota
                            str(row_pro[7] if len(row_pro) > 7 else ""),   # H: Total Cuotas
                            cartera,                                       # I: Cartera
                            "",                                            # J: Cartera Cta.
                            "",                                            # K: Producto Cta.
                            str(row_pro[4] if len(row_pro) > 4 else ""),   # L: Operador
                            detectar_entidades(cartera_raw),               # M: Entidad
                            "",                                            # N: Cta. Destino (se pisa)  
                            "",                                            # O: Observaciones
                            "",                                            # P: Transferido (se pisa)
                            dia_habil,                                     # Q: Nº Día
                            str(row_pro[8] if len(row_pro) > 8 else ""),   # R: ID
                            1,                                             # S: Tipo Doc
                            it["dni"],                                     # T: NUMEDOCU
                            fecha_fmt,                                     # U: FECHPAGO
                            "",                                            # V: MONTPAGO (se pisa)
                            "",                                            # W: TPO_ORIG (se pisa)
                            "",                                            # X: MANGO
                            "",                                            # Y: Honorario (se pisa)
                        ]
                        base_from = "PRO"
                    else:
                        sin_base += 1
                        marcas_q[it["idx"]] = "❌ HON ERROR: Sin registro base (mes actual, anterior ni PRO)"
                        continue

            # Asegurar longitud exacta
            while len(base_row) < COLS_OUT:
                base_row.append("")
            base_row = base_row[:COLS_OUT]

            # Modificar para honorario
            importe_fmt = extraer_solo_numeros_crudos(it["monto_raw"])
            monto_sin = limpiar_monto_sin_decimales(it["monto_raw"])

            base_row[2] = fecha_fmt     # C Fecha
            base_row[3] = importe_fmt   # D Importe
            base_row[13] = "Banco"      # N
            base_row[14] = ""           # O Observaciones en blanco
            base_row[15] = ""           # P limpiar estado (Transferido)
            base_row[16] = dia_habil    # Q Nº Día
            base_row[20] = fecha_fmt    # U FECHPAGO
            base_row[21] = monto_sin    # V MONTPAGO
            base_row[22] = 2            # W TPO_ORIG
            base_row[23] = ""           # X VACÍA (regla tuya)
            base_row[24] = True         # Y honorario

            appends.append(base_row)

            # ✅ sumar al set para evitar duplicados dentro del mismo run
            honorario_keys.add(dup_key)
            procesados += 1

            if base_from != hoja_nombre:
                marcas_q[it["idx"]] = f"✅ HON OK → {hoja_nombre} (base: {base_from})"
            else:
                marcas_q[it["idx"]] = f"✅ HON OK → {hoja_nombre}"

        # Escritura batch única
        if appends:
            sheets.escribir_filas_mes(hoja_nombre, appends)

    # =========================================================
    # 3) LIMPIAR + ESCRIBIR ESTADOS EN Q (vector completo)
    # =========================================================
    logs_q = [""] * len(info_data)
    for idx, txt in marcas_q.items():
        if 0 <= idx < len(logs_q):
            logs_q[idx] = txt

    # ✅ Limpia y luego escribe Q completa
    sheets.limpiar_y_escribir_info_col_q(logs_q)

    logger.info(f"Honorarios finalizado: OK={procesados}, DUP={duplicados}, SIN_BASE={sin_base}")
    return {"procesados": procesados, "duplicados": duplicados, "sin_base": sin_base}
