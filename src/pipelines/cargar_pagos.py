"""
cargar_pagos.py — Pipeline principal de carga de pagos
Lee 'Informacion imagenes' + 'Pro', mapea por DNI, escribe en hojas mensuales.
"""

import logging
import re
import unicodedata
from typing import Dict, Any, List

from src.utils.sheets_io import SheetsIO
from src.utils.config import MESES_ES, MESES_ABREV, CANAL_TO_CODE
from src.utils.parsers import (
    extraer_dni_desde_archivo,
    parsear_fecha_flexible,
    nombre_hoja_mes,
    formato_fecha_corta,
)
from src.utils.text import (
    extraer_solo_numeros_crudos,
    limpiar_monto_sin_decimales,
    parsear_monto_float,
    normalize_canal,
    contiene_pago_facil,
    contiene_rapipago,
    es_banco_comafi,
    detectar_entidades,
)
from src.utils.calendar_ar import calcular_dia_habil_del_mes

logger = logging.getLogger(__name__)

# =============================================================================
# Robustez Columna N (Cta. Destino)
# - Prioridad: emisor → Banco / Pago Facil / Rapipago (como ya funcionaba)
# - Si NO cae en esas 3: mapear destino_raw por patrones → namePago
# - Si no hay match: fallback destino_raw
# =============================================================================

DESTINATARIOS = [
    {
        "namePago": "Cta. Comafi",
        "patrones": [
            "2990000000001054390008", "10543/9", "30-60473101-8", "30604731018", "2900000000001054300000", "MOZO.TAPA.ROSTRO",
            "29900000-00001054390008"
        ]
    },
    {
        "namePago": "Banco",
        "patrones": [
            "0170155120000001255595", "155-012555/9", "30-71631686-2", "30716316862", "cobranzas.bfb"
        ]
    },
    {
        "namePago": "Cta. Creditia",
        "patrones": [
            "099-720777/6", "0170099220000072077766",
            "(000) 033600/2", "0720000720000003360022", "30-71213737-8", "30-71789342-1", "10600114736728"
        ]
    },
    {
        "namePago": "Cta. Mins",
        "patrones": [
            "2990113311300150410002", "30-71683093-0", "30716830930"
        ]
    },
    {
        "namePago": "Cta. RDA",
        "patrones": [
            "0004194-6 024-7", "0070024520000004194671", "33-71573296-9 "
        ]
    },
    {
        "namePago": "Cta. Exi",
        "patrones": [
            "18156-5 339-3", "0070339820000018156535", "30-71589360-2",
            "30-71851349-5", "2850793630094217184081"
        ]
    },
    {
        "namePago": "Cta. Efectivo Si",
        "patrones": [
            "30538006404", "27123601659", "0720000720000001663918", "27123601659"
        ]
    }
]

_SEP_RX = re.compile(r"[\s\-\./()]+")
_DIGITS_RX = re.compile(r"\D+")


def _norm_txt(s: str) -> str:
    """Lower + sin acentos + trim."""
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s


def _compact_txt(s: str) -> str:
    """Texto compacto sin separadores típicos (espacios, guiones, puntos, barras, paréntesis)."""
    return _SEP_RX.sub("", _norm_txt(s))


def _only_digits(s: str) -> str:
    """Solo dígitos (útil para CUIT/CBU)."""
    return _DIGITS_RX.sub("", s or "")


# Precompilado de patrones (para velocidad + robustez)
DESTINATARIOS_COMPILED = []
for d in DESTINATARIOS:
    pats_compact = [_compact_txt(p) for p in d.get("patrones", []) if p]
    pats_digits = [_only_digits(p) for p in d.get("patrones", []) if _only_digits(p)]
    DESTINATARIOS_COMPILED.append({
        "namePago": d["namePago"],
        "p_compact": [p for p in pats_compact if p],
        "p_digits": [p for p in pats_digits if p],
    })


def resolver_cta_destino(emisor: str, destino_raw: str) -> str:
    """
    Resuelve el texto final para la columna N (Cta. Destino).

    Reglas:
    1) Mantener prioridad existente por emisor:
       - Comafi/Banco → "Banco"
       - Pago Facil → "Pago Facil"
       - Rapipago → "Rapipago"

    2) Si NO es ninguno de los 3:
       - Buscar patrones en destino_raw y devolver el namePago correspondiente.

    3) Fallback:
       - devolver destino_raw tal cual (strip)
    """
    raw = (destino_raw or "").strip()

    # (1) Prioridad existente por emisor (NO cambiar comportamiento actual)
    if es_banco_comafi(emisor):
        return "Banco"
    if contiene_pago_facil(emisor):
        return "Pago Facil"
    if contiene_rapipago(emisor):
        return "Rapipago"

    # (2) Match por patrones contra destino_raw
    raw_compact = _compact_txt(raw)
    raw_digits = _only_digits(raw)

    for d in DESTINATARIOS_COMPILED:
        for p in d["p_compact"]:
            if p and p in raw_compact:
                return d["namePago"]
        for p in d["p_digits"]:
            if p and p in raw_digits:
                return d["namePago"]

    # (3) Fallback
    return raw


def _tipo_pago(valor: str) -> str:
    """Determina tipo de pago desde texto de Pro."""
    txt = str(valor or "").lower().strip()
    if "cuota" in txt:
        return "Cuota"
    if "parcial" in txt:
        return "Parcial"
    if "cancelación" in txt or "cancelacion" in txt or "total" in txt:
        return "Total"
    if "adelanto" in txt:
        return "Adelanto/Anticipo"
    return "No reconocido"


def _codigo_tipo(tipo: str) -> str:
    """Código para columna F."""
    if tipo == "Total":
        return "PGTOT"
    if tipo in ("Adelanto/Anticipo", "Cuota"):
        return "PGPREF"
    if tipo == "Parcial":
        return "PGPR"
    return "VER"


def _contar_cuotas_pagadas(row_pro: List, header_pro_lower: List[str], mes_nombre: str) -> int:
    """Cuenta cuántas columnas 'pago' anteriores al mes actual tienen valor."""
    pago_col_idx = -1
    for k, h in enumerate(header_pro_lower):
        if "pago" in h and mes_nombre in h:
            pago_col_idx = k
            break

    cuotas = 0
    if pago_col_idx > 0:
        for k in range(pago_col_idx):
            if header_pro_lower[k].startswith("pago"):
                try:
                    val = float(str(row_pro[k]).replace(".", "").replace(",", "."))
                    if val > 0:
                        cuotas += 1
                except (ValueError, TypeError):
                    pass
    return cuotas


def ejecutar_carga_pagos(sheets: SheetsIO) -> Dict[str, Any]:
    """
    Pipeline principal:
    1. Lee 'Informacion imagenes' y 'Pro'
    2. Mapea cada comprobante por DNI
    3. Escribe en hojas mensuales
    4. Retorna estadísticas
    """
    # Leer datos
    info_data = sheets.leer_info_imagenes()
    header_pro, data_pro = sheets.leer_pro()

    if not info_data:
        logger.info("Sin datos en 'Informacion imagenes'")
        return {"cargados": 0, "duplicados": 0, "errores": 0}

    if not data_pro:
        logger.warning("Sin datos en 'Pro'")
        return {"cargados": 0, "duplicados": 0, "errores": 0, "detalle_errores": "Hoja Pro vacía"}

    # Mapeo DNI → índice en Pro
    header_pro_lower = [str(h or "").lower() for h in header_pro]
    mapa_dni = {}
    for i, row in enumerate(data_pro):
        dni = str(row[2] if len(row) > 2 else "").strip()
        if dni:
            mapa_dni[dni] = i

    # Cargar entradas existentes para deduplicación
    existing_entries = {}  # {nombre_hoja: set de "dni|dia|mesIdx"}
    filas_por_hoja = {}    # {nombre_hoja: [filas]}
    estados = []

    cargados = duplicados = errores = 0

    for i, row in enumerate(info_data):
        try:
            # ─── Extraer DNI ───
            archivo = str(row[0] if len(row) > 0 else "").strip()
            dni = extraer_dni_desde_archivo(archivo)

            if not dni:
                estados.append("DNI inválido en archivo")
                continue

            if dni not in mapa_dni:
                estados.append("No existe en PRO")
                errores += 1
                continue

            # ─── Parsear fecha ───
            fecha_raw = row[3] if len(row) > 3 else None
            fecha = parsear_fecha_flexible(fecha_raw)
            if not fecha:
                estados.append("Fecha inválida")
                errores += 1
                continue

            dia = fecha["dia"]
            mes_idx = fecha["mesIdx"]
            anio = fecha["anio"]

            # ─── Hoja destino ───
            hoja_nombre = nombre_hoja_mes(mes_idx, anio)

            # Lazy load existing entries
            if hoja_nombre not in existing_entries:
                existing_entries[hoja_nombre] = set()
                data_mes = sheets.leer_hoja_sin_header(hoja_nombre) if hoja_nombre in [s.title for s in sheets.sh.worksheets()] else []
                for r in data_mes:
                    if len(r) > 2:
                        dni_exist = str(r[0]).strip()
                        from src.utils.parsers import parsear_dia_mes_texto
                        parsed = parsear_dia_mes_texto(r[2]) if isinstance(r[2], str) else None
                        if dni_exist and parsed:
                            existing_entries[hoja_nombre].add(f"{dni_exist}|{parsed['dia']}|{parsed['mesIdx']}")

            if hoja_nombre not in filas_por_hoja:
                filas_por_hoja[hoja_nombre] = []

            # ─── Deduplicación ───
            clave = f"{dni}|{dia}|{mes_idx}"
            if clave in existing_entries[hoja_nombre]:
                estados.append(f"Duplicado en {hoja_nombre}")
                duplicados += 1
                continue

            # ─── Datos del cliente (Pro) ───
            row_pro = data_pro[mapa_dni[dni]]

            # Monto
            monto_raw = row[5] if len(row) > 5 else ""
            monto_formateado = extraer_solo_numeros_crudos(monto_raw)
            monto_num = parsear_monto_float(monto_raw)

            # Emisor y canal
            emisor = str(row[2] if len(row) > 2 else "").strip()
            destino_raw = str(row[10] if len(row) > 10 else "").strip()

            if not destino_raw:
                destino_raw = str(row[10] if len(row) > 10 else "").strip()

            # ✅ ÚNICO CAMBIO FUNCIONAL: Columna N robusta
            destino_n = resolver_cta_destino(emisor, destino_raw)

            # Mantener W con el mismo mecanismo (pero usando el valor final de N)
            canal_code = CANAL_TO_CODE.get(normalize_canal(destino_n), 0)

            # Tipo de pago
            tipo_texto = str(row_pro[5] if len(row_pro) > 5 else "").strip()
            tipo = _tipo_pago(tipo_texto)

            total_pactado = parsear_monto_float(row_pro[7] if len(row_pro) > 7 else "")
            valor_cuota = parsear_monto_float(row_pro[8] if len(row_pro) > 8 else "")

            if tipo == "Total" and monto_num > 0 and monto_num < total_pactado:
                tipo = "Parcial"
            if tipo == "Cuota" and monto_num > 0 and monto_num < valor_cuota:
                tipo = "Parcial"

            col_f = _codigo_tipo(tipo)

            # Cuotas pagadas
            mes_nombre = MESES_ES[mes_idx].lower()
            cuotas_pagadas = _contar_cuotas_pagadas(row_pro, header_pro_lower, mes_nombre)

            # Cartera
            cartera_raw = str(row_pro[9] if len(row_pro) > 9 else "").strip()
            cartera = "Comafi" if es_banco_comafi(cartera_raw) else cartera_raw

            # Día hábil
            dia_habil = calcular_dia_habil_del_mes(dia, mes_idx, anio) or ""

            # Fecha formateada
            fecha_fmt = formato_fecha_corta(dia, mes_idx)

            # ─── Construir fila (25 columnas) ───
            nueva_fila = [
                dni,                                              # A: DNI
                str(row_pro[3] if len(row_pro) > 3 else ""),     # B: Nombre
                fecha_fmt,                                         # C: Fecha
                monto_formateado,                                  # D: Importe
                tipo,                                              # E: Concepto
                col_f,                                             # F: Tipo de Pago
                cuotas_pagadas + 1,                                # G: Nro de Cuota
                str(row_pro[7] if len(row_pro) > 7 else ""),     # H: Total Cuotas
                cartera,                                           # I: Cartera
                "",                                                # J: Cartera Cta.
                "",                                                # K: Producto Cta.
                str(row_pro[4] if len(row_pro) > 4 else ""),     # L: Operador
                detectar_entidades(cartera_raw),                   # M: Entidad
                destino_n,                                         # N: Cta. Destino  
                "",                                                # O: Observaciones
                "",                                                # P: Transferido
                dia_habil,                                         # Q: Nº Día
                str(row_pro[8] if len(row_pro) > 8 else ""),     # R: ID
                1,                                                 # S: Tipo Doc
                dni,                                               # T: NUMEDOCU
                fecha_fmt,                                         # U: FECHPAGO
                limpiar_monto_sin_decimales(monto_raw),           # V: MONTPAGO
                canal_code,                                        # W: TPO_ORIG
                "",                                                # X: MANGO
                "",                                                # Y: Honorario
            ]

            filas_por_hoja[hoja_nombre].append(nueva_fila)
            existing_entries[hoja_nombre].add(clave)
            estados.append(f"OK ({hoja_nombre})")
            cargados += 1

        except Exception as e:
            estados.append(f"Error: {e}")
            errores += 1
            logger.exception(f"Error fila {i}: {e}")

    # ─── Escritura batch ───
    for hoja_nombre, filas in filas_por_hoja.items():
        if filas:
            sheets.escribir_filas_mes(hoja_nombre, filas)

    # Escribir estados en columna P
    sheets.escribir_estado_info(estados)

    logger.info(f"Carga: {cargados} OK, {duplicados} dup, {errores} err")
    return {
        "cargados": cargados,
        "duplicados": duplicados,
        "errores": errores,
        "detalle_errores": None if errores == 0 else f"{errores} filas con error",
    }
