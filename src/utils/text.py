"""
text.py — Normalización de texto, limpieza de montos, detección de canales
"""

import re
import unicodedata


def normalize(txt: str) -> str:
    """Normaliza texto: sin acentos, sin espacios, lowercase."""
    if not txt:
        return ""
    return (
        unicodedata.normalize("NFD", str(txt))
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
        .strip()
    )


def normalize_canal(txt: str) -> str:
    """Normaliza para matching de canal de pago."""
    if not txt:
        return ""
    return re.sub(
        r"[^a-z0-9]",
        "",
        unicodedata.normalize("NFD", str(txt))
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower(),
    )


def extraer_solo_numeros_crudos(texto) -> str:
    """
    Extrae el monto como string preservando puntos y comas.
    Ej: '$ 90.000,50' → '90.000,50'
    """
    s = re.sub(r"[^\d.,]", "", str(texto or ""))
    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_dot > last_comma:
            # Formato americano → invertir
            s = s.replace(".", "§").replace(",", ".").replace("§", ",")
    return s


def limpiar_monto_sin_decimales(texto) -> str:
    """
    Limpia monto quitando decimales y puntos de miles.
    Ej: '90.000,50' → '90000'
    """
    s = extraer_solo_numeros_crudos(texto)
    idx = s.rfind(",")
    if idx != -1:
        s = s[:idx]
    return s.replace(".", "")


def parsear_monto_float(texto) -> float:
    """
    Parsea monto argentino a float.
    '90.000,50' → 90000.50
    '120000' → 120000.0
    """
    if texto is None:
        return 0.0
    s = str(texto).strip()
    s = re.sub(r"[^\d.,]", "", s)
    if not s:
        return 0.0

    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "." in s:
        parts = s.split(".")
        if len(parts) > 2:
            s = s.replace(".", "")
        elif len(parts) == 2 and len(parts[1]) == 3:
            s = s.replace(".", "")
        elif len(parts) == 2 and len(parts[1]) >= 4:
            s = s.replace(".", "")

    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def contiene_pago_facil(txt: str) -> bool:
    return "pagofacil" in normalize_canal(txt or "")


def contiene_rapipago(txt: str) -> bool:
    return "rapipago" in normalize_canal(txt or "")


def es_banco_comafi(txt: str) -> bool:
    n = normalize_canal(txt or "")
    return "bancocomafi" in n or "comafi" in n


def detectar_entidades(txt: str) -> str:
    """Detecta entidades conocidas en el texto."""
    from src.utils.config import ENTIDADES_CONOCIDAS

    upper = str(txt or "").upper()
    found = [e for e in ENTIDADES_CONOCIDAS if e in upper]
    return " ".join(found)
