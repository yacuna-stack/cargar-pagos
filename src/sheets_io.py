"""
sheets_io.py — Lectura y escritura centralizada de Google Sheets
Usa la API de Sheets v4 para operaciones batch eficientes.
"""

import logging
from typing import List, Any, Optional, Dict

import gspread
import google.auth
from google.auth.transport.requests import Request as AuthRequest

from src.utils.config import HEADERS_MES

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Credenciales globales
_creds, _ = google.auth.default(scopes=SCOPES)
_auth_req = AuthRequest()


def _refresh():
    if not _creds.valid:
        _creds.refresh(_auth_req)


class SheetsIO:
    """Maneja toda la interacción con Google Sheets."""

    def __init__(self, spreadsheet_id: str):
        _refresh()
        self.gc = gspread.authorize(_creds)
        self.sh = self.gc.open_by_key(spreadsheet_id)
        self._hoja_cache: Dict[str, gspread.Worksheet] = {}

    # ─── Lectura ───

    def leer_hoja(self, nombre: str) -> List[List[Any]]:
        """Lee todos los datos de una hoja (incluye header)."""
        try:
            ws = self.sh.worksheet(nombre)
            return ws.get_all_values()
        except gspread.exceptions.WorksheetNotFound:
            return []

    def leer_hoja_sin_header(self, nombre: str) -> List[List[Any]]:
        """Lee datos sin la fila de header."""
        data = self.leer_hoja(nombre)
        return data[1:] if len(data) > 1 else []

    def leer_info_imagenes(self) -> List[List[Any]]:
        """Lee 'Informacion imagenes' sin header."""
        return self.leer_hoja_sin_header("Informacion imagenes")

    def leer_pro(self) -> tuple:
        """Lee hoja 'Pro'. Retorna (header, data_rows)."""
        data = self.leer_hoja("Pro")
        if not data:
            return [], []
        return data[0], data[1:]

    # ─── Hojas mensuales ───

    def obtener_o_crear_hoja_mes(self, nombre: str) -> gspread.Worksheet:
        """Obtiene o crea una hoja mensual con headers."""
        if nombre in self._hoja_cache:
            return self._hoja_cache[nombre]

        try:
            ws = self.sh.worksheet(nombre)
        except gspread.exceptions.WorksheetNotFound:
            ws = self.sh.add_worksheet(title=nombre, rows=1000, cols=len(HEADERS_MES))
            ws.append_row(HEADERS_MES, value_input_option="USER_ENTERED")
            logger.info(f"Creada hoja '{nombre}'")

        self._hoja_cache[nombre] = ws
        return ws

    def leer_hoja_mes(self, nombre: str) -> List[List[Any]]:
        """Lee datos de una hoja mensual (con header)."""
        ws = self.obtener_o_crear_hoja_mes(nombre)
        return ws.get_all_values()

    def escribir_filas_mes(self, nombre: str, filas: List[List[Any]]):
        """Escribe filas al final de una hoja mensual."""
        if not filas:
            return
        ws = self.obtener_o_crear_hoja_mes(nombre)
        ws.append_rows(filas, value_input_option="USER_ENTERED")
        logger.info(f"Escritas {len(filas)} filas en '{nombre}'")

    def actualizar_columna_mes(self, nombre: str, col_idx: int, valores: List):
        """Actualiza una columna completa (desde fila 2) en una hoja mensual."""
        if not valores:
            return
        ws = self.obtener_o_crear_hoja_mes(nombre)
        # col_idx es 0-based, gspread usa 1-based
        cell_list = [[v] for v in valores]
        col_letter = chr(65 + col_idx) if col_idx < 26 else f"A{chr(65 + col_idx - 26)}"
        rng = f"{col_letter}2:{col_letter}{len(valores) + 1}"
        ws.update(rng, cell_list, value_input_option="USER_ENTERED")

    def actualizar_dos_columnas_mes(self, nombre: str, col_e_vals: List, col_g_vals: List):
        """Actualiza columnas E (concepto) y G (cuota) de la hoja mensual."""
        ws = self.obtener_o_crear_hoja_mes(nombre)
        total = len(col_e_vals)
        if total == 0:
            return
        # E = col 5 (1-based)
        ws.update(f"E2:E{total + 1}", [[v] for v in col_e_vals], value_input_option="USER_ENTERED")
        # G = col 7 (1-based)
        ws.update(f"G2:G{total + 1}", [[v] for v in col_g_vals], value_input_option="USER_ENTERED")

    # ─── Escritura general ───

    def escribir_estado_info(self, estados: List[str], col: int = 16):
        """Escribe estados en columna P (16) de 'Informacion imagenes'."""
        if not estados:
            return
        try:
            ws = self.sh.worksheet("Informacion imagenes")
            cell_list = [[s] for s in estados]
            col_letter = chr(64 + col)
            ws.update(f"{col_letter}2:{col_letter}{len(estados) + 1}", cell_list, value_input_option="USER_ENTERED")
        except Exception as e:
            logger.warning(f"Error escribiendo estados: {e}")

    # ─── Histórico ───

    def copiar_a_historico(self, filas: List[List[Any]], header: List[str]):
        """Copia filas a hoja 'Historico' (crea si no existe)."""
        if not filas:
            return
        try:
            ws = self.sh.worksheet("Historico")
        except gspread.exceptions.WorksheetNotFound:
            ws = self.sh.add_worksheet(title="Historico", rows=1000, cols=len(header))
            ws.append_row(header, value_input_option="RAW")
            logger.info("Creada hoja 'Historico'")

        ws.append_rows(filas, value_input_option="RAW")
        logger.info(f"Copiadas {len(filas)} filas a 'Historico'")