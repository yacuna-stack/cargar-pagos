"""
historico.py — Copia datos de 'Informacion imagenes' a 'Historico'
"""

import logging
from src.utils.sheets_io import SheetsIO

logger = logging.getLogger(__name__)


def ejecutar_historico(sheets: SheetsIO):
    """Copia todas las filas de 'Informacion imagenes' a 'Historico'."""
    data = sheets.leer_hoja("Informacion imagenes")
    if len(data) <= 1:
        logger.info("Sin datos para copiar a Historico")
        return

    header = data[0][:16]
    filas = [row[:16] for row in data[1:]]

    sheets.copiar_a_historico(filas, header)
    logger.info(f"Historico: {len(filas)} filas copiadas")