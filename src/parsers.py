"""
parsers.py — Parseo de fechas y extracción de DNI desde filenames
"""

import re
from datetime import datetime
from typing import Optional, Dict

from src.utils.config import MESES_ABREV, MESES_ES


def extraer_dni_desde_archivo(archivo: str) -> str:
    """
    Extrae DNI (6-12 dígitos) del nombre de archivo.
    Si contiene 'h' o 'H' como marca de honorario, retorna '' (se procesa aparte).
    """
    if not archivo:
        return ""
    s = str(archivo).strip()

    # Si contiene H/h como marca → descartar (es honorario)
    if re.search(r"\b[a-z]*h\b", s, re.IGNORECASE):
        return ""

    # Buscar DNI al inicio
    m = re.match(r"^\s*(\d{6,12})\b", s)
    if m:
        return m.group(1)

    # Buscar DNI en cualquier parte
    m = re.search(r"(\d{6,12})", s)
    return m.group(1) if m else ""


def extraer_dni_honorario(archivo: str) -> str:
    """
    Extrae DNI de archivos con patrón '<DNI> h' o '<DNI> H'.
    Retorna DNI si es honorario, '' si no.
    """
    if not archivo:
        return ""
    m = re.match(r"^\s*(\d{6,12})\s*[hH]\b", str(archivo).strip())
    return m.group(1) if m else ""


def parsear_fecha_flexible(input_val) -> Optional[Dict]:
    """
    Parsea fecha en múltiples formatos. Retorna {dia, mesIdx, anio} o None.
    mesIdx es 0-based (0=Enero, 11=Diciembre).
    """
    if input_val is None:
        return None

    # Si ya es datetime
    if isinstance(input_val, datetime):
        return {"dia": input_val.day, "mesIdx": input_val.month - 1, "anio": input_val.year}

    s = str(input_val).strip()
    if not s:
        return None

    # DD/MM/YYYY o DD-MM-YYYY (con hora opcional)
    m = re.match(r"^(\d{1,2})[/\-\s](\d{1,2})[/\-\s](\d{2,4})(?:[ T].*)?$", s)
    if m:
        dia = int(m.group(1))
        mes = int(m.group(2)) - 1
        anio = int(m.group(3))
        if anio < 100:
            anio += 2000
        if 1 <= dia <= 31 and 0 <= mes <= 11:
            return {"dia": dia, "mesIdx": mes, "anio": anio}

    # YYYY-MM-DD (ISO)
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:[T\s].*)?$", s)
    if m:
        anio = int(m.group(1))
        mes = int(m.group(2)) - 1
        dia = int(m.group(3))
        if 1 <= dia <= 31 and 0 <= mes <= 11:
            return {"dia": dia, "mesIdx": mes, "anio": anio}

    # DD-mmm (ej: "07-feb")
    parsed = parsear_dia_mes_texto(s)
    if parsed:
        now = datetime.now()
        return {"dia": parsed["dia"], "mesIdx": parsed["mesIdx"], "anio": now.year}

    # Fallback: Python date parser
    try:
        dt = datetime.fromisoformat(s)
        return {"dia": dt.day, "mesIdx": dt.month - 1, "anio": dt.year}
    except (ValueError, TypeError):
        pass

    return None


def parsear_dia_mes_texto(texto: str) -> Optional[Dict]:
    """
    Parsea formato 'DD-mmm' (ej: '07-feb').
    Retorna {dia, mesIdx} o None.
    """
    t = str(texto or "").strip().lower()
    parts = t.split("-")
    if len(parts) != 2:
        return None

    try:
        dia = int(parts[0])
    except ValueError:
        return None

    mes_txt = parts[1].strip()
    # Alias
    if mes_txt == "sept":
        mes_txt = "sep"

    if mes_txt in MESES_ABREV:
        mes_idx = MESES_ABREV.index(mes_txt)
        if 1 <= dia <= 31:
            return {"dia": dia, "mesIdx": mes_idx}

    return None


def nombre_hoja_mes(mes_idx: int, anio: int) -> str:
    """Genera nombre de hoja: 'Febrero 26'."""
    anio_corto = str(anio)[-2:]
    return f"{MESES_ES[mes_idx]} {anio_corto}"


def formato_fecha_corta(dia: int, mes_idx: int) -> str:
    """Formatea fecha como 'DD-mmm' (ej: '07-feb')."""
    return f"{str(dia).zfill(2)}-{MESES_ABREV[mes_idx]}"