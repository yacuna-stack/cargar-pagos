"""
calendar_ar.py — Feriados argentinos + cálculo de días hábiles
"""

import logging
from datetime import date, timedelta
from typing import Set, Optional

logger = logging.getLogger(__name__)

# Cache de feriados por año
_feriados_cache: dict = {}

# Fallback hardcodeado
FERIADOS_FALLBACK = {
    2024: [
        "2024-01-01", "2024-02-12", "2024-02-13", "2024-03-24", "2024-03-29",
        "2024-04-02", "2024-05-01", "2024-05-25", "2024-06-17", "2024-06-20",
        "2024-07-09", "2024-08-17", "2024-10-12", "2024-11-18", "2024-12-08",
        "2024-12-25",
    ],
    2025: [
        "2025-01-01", "2025-03-03", "2025-03-04", "2025-03-24", "2025-04-02",
        "2025-04-18", "2025-05-01", "2025-05-25", "2025-06-16", "2025-06-20",
        "2025-07-09", "2025-08-18", "2025-10-13", "2025-11-24", "2025-12-08",
        "2025-12-25",
    ],
    2026: [
        "2026-01-01", "2026-02-16", "2026-02-17", "2026-03-24", "2026-04-02",
        "2026-04-03", "2026-05-01", "2026-05-25", "2026-06-15", "2026-06-20",
        "2026-07-09", "2026-08-17", "2026-10-12", "2026-11-23", "2026-12-08",
        "2026-12-25",
    ],
}


def get_feriados(year: int) -> Set[str]:
    """Retorna set de feriados como 'YYYY-MM-DD' para el año dado."""
    if year in _feriados_cache:
        return _feriados_cache[year]

    feriados = set()

    # Intentar API
    try:
        import requests
        resp = requests.get(
            f"https://date.nager.at/api/v3/PublicHolidays/{year}/AR",
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            feriados = {f["date"] for f in data}
            logger.info(f"Feriados {year} desde API: {len(feriados)}")
    except Exception as e:
        logger.warning(f"API feriados falló para {year}: {e}")

    # Merge con fallback
    fallback = set(FERIADOS_FALLBACK.get(year, []))
    feriados = feriados | fallback

    _feriados_cache[year] = feriados
    return feriados


def es_dia_habil(d: date) -> bool:
    """True si no es fin de semana ni feriado."""
    if d.weekday() >= 5:  # 5=sáb, 6=dom
        return False
    feriados = get_feriados(d.year)
    return d.isoformat() not in feriados


def proximo_dia_habil(d: date) -> date:
    """Avanza al próximo día hábil si cae en finde/feriado."""
    while not es_dia_habil(d):
        d = d + timedelta(days=1)
    return d


def calcular_dia_habil_del_mes(dia: int, mes: int, anio: int) -> Optional[int]:
    """
    Calcula cuántos días hábiles van del 1 al día dado del mes.
    Si cae en finde/feriado, avanza al próximo hábil.
    Retorna el número de día hábil (1-based) o None.
    """
    try:
        fecha_original = date(anio, mes + 1, dia)  # mes+1 porque mesIdx es 0-based
    except (ValueError, TypeError):
        return None

    # Mover a día hábil si es necesario
    fecha = proximo_dia_habil(fecha_original)

    # Si cruzó de año, recargar feriados
    feriados = get_feriados(fecha.year)

    # Contar hábiles del 1 al día
    habiles = 0
    for d_num in range(1, fecha.day + 1):
        d = date(fecha.year, fecha.month, d_num)
        if d.weekday() < 5 and d.isoformat() not in feriados:
            habiles += 1

    return habiles if habiles > 0 else None