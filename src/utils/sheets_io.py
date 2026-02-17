"""
sheets_io.py — Lectura y escritura centralizada de Google Sheets
Usa gspread para operaciones batch eficientes.
"""

import logging
from typing import List, Any, Dict

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

    def actualizar_columna_mes(self, nombre: str, col_idx: int, valores: List[Any]):
        """Actualiza una columna completa (desde fila 2) en una hoja mensual."""
        if not valores:
            return
        ws = self.obtener_o_crear_hoja_mes(nombre)

        cell_list = [[v] for v in valores]

        # col_idx es 0-based; convertir a letra A..Z, AA.. (simple y correcto)
        col_num = col_idx + 1  # 1-based
        col_letter = gspread.utils.rowcol_to_a1(1, col_num).rstrip("1")  # "A1" -> "A"

        rng = f"{col_letter}2:{col_letter}{len(valores) + 1}"
        ws.update(rng, cell_list, value_input_option="USER_ENTERED")

    def actualizar_dos_columnas_mes(self, nombre: str, col_e_vals: List[Any], col_g_vals: List[Any]):
        """Actualiza columnas E (concepto) y G (cuota) de la hoja mensual."""
        ws = self.obtener_o_crear_hoja_mes(nombre)
        total = len(col_e_vals)
        if total == 0:
            return

        ws.update(f"E2:E{total + 1}", [[v] for v in col_e_vals], value_input_option="USER_ENTERED")
        ws.update(f"G2:G{total + 1}", [[v] for v in col_g_vals], value_input_option="USER_ENTERED")

    # ─── Escritura general ───

    def escribir_estado_info(self, estados: List[str], col: int = 16):
        """
        Escribe estados en una columna de 'Informacion imagenes'.
        Por defecto col=16 => P.
        """
        if not estados:
            return
        try:
            ws = self.sh.worksheet("Informacion imagenes")
            cell_list = [[s] for s in estados]
            col_letter = chr(64 + col)  # 1->A, 16->P
            ws.update(f"{col_letter}2:{col_letter}{len(estados) + 1}", cell_list, value_input_option="USER_ENTERED")
        except Exception as e:
            logger.warning(f"Error escribiendo estados: {e}")

    def escribir_estado_info_imagenes_col_q(self, marcas_q: Dict[int, str]):
        """
        Escribe estados SOLO en las filas indicadas en 'Informacion imagenes', columna Q.

        - marcas_q: dict {idx_0based: "texto"}, donde idx_0based corresponde a leer_info_imagenes()
          (fila real = idx + 2 porque fila 1 es header).
        - No pisa otras filas no incluidas.
        """
        if not marcas_q:
            return

        try:
            ws = self.sh.worksheet("Informacion imagenes")

            updates = []
            for idx, txt in marcas_q.items():
                row_num = int(idx) + 2  # idx 0 -> fila 2
                updates.append({"range": f"Q{row_num}", "values": [[str(txt)]]})

            # batch_update pisa solo esas celdas
            ws.batch_update(updates, value_input_option="USER_ENTERED")

        except Exception as e:
            logger.warning(f"Error escribiendo estados honorarios en Q: {e}")

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
