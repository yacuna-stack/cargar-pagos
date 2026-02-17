"""
sheets_io.py — Lectura y escritura centralizada de Google Sheets
Usa gspread para operaciones batch eficientes.
"""

import logging
from typing import List, Any, Dict, Tuple

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

    # ─────────────────────────────────────────────────────────
    # Helpers internos
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _col_letter(col_1based: int) -> str:
        """Convierte número de columna 1-based a letra (A, B, ..., AA, AB...)."""
        return gspread.utils.rowcol_to_a1(1, col_1based).rstrip("1")

    def _get_ws(self, nombre: str) -> gspread.Worksheet:
        """Obtiene worksheet por nombre (sin cache, para hojas no mensuales)."""
        return self.sh.worksheet(nombre)

    # ─────────────────────────────────────────────────────────
    # Lectura
    # ─────────────────────────────────────────────────────────

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

    def leer_pro(self) -> Tuple[List[Any], List[List[Any]]]:
        """Lee hoja 'Pro'. Retorna (header, data_rows)."""
        data = self.leer_hoja("Pro")
        if not data:
            return [], []
        return data[0], data[1:]

    # ─────────────────────────────────────────────────────────
    # Hojas mensuales
    # ─────────────────────────────────────────────────────────

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

        # col_idx es 0-based; pasar a 1-based y convertir a letra
        col_num = col_idx + 1
        col_letter = self._col_letter(col_num)

        rng = f"{col_letter}2:{col_letter}{len(valores) + 1}"
        ws.update(rng, [[v] for v in valores], value_input_option="USER_ENTERED")

    def actualizar_dos_columnas_mes(self, nombre: str, col_e_vals: List[Any], col_g_vals: List[Any]):
        """Actualiza columnas E (concepto) y G (cuota) de la hoja mensual."""
        ws = self.obtener_o_crear_hoja_mes(nombre)
        total = len(col_e_vals)
        if total == 0:
            return

        ws.update(f"E2:E{total + 1}", [[v] for v in col_e_vals], value_input_option="USER_ENTERED")
        ws.update(f"G2:G{total + 1}", [[v] for v in col_g_vals], value_input_option="USER_ENTERED")

    # ─────────────────────────────────────────────────────────
    # Escritura general
    # ─────────────────────────────────────────────────────────

    def escribir_estado_info(self, estados: List[str], col: int = 16):
        """
        Escribe estados en una columna de 'Informacion imagenes'.
        Por defecto col=16 => P.
        """
        if not estados:
            return
        try:
            ws = self.sh.worksheet("Informacion imagenes")

            # Asegurar que exista la columna
            if ws.col_count < col:
                ws.add_cols(col - ws.col_count)

            col_letter = self._col_letter(col)
            ws.update(
                f"{col_letter}2:{col_letter}{len(estados) + 1}",
                [[s] for s in estados],
                value_input_option="USER_ENTERED",
            )
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

            # Asegurar que exista la columna Q (17)
            if ws.col_count < 17:
                ws.add_cols(17 - ws.col_count)

            updates = []
            for idx, txt in marcas_q.items():
                row_num = int(idx) + 2  # idx 0 -> fila 2
                updates.append({"range": f"Q{row_num}", "values": [[str(txt)]]})

            ws.batch_update(updates, value_input_option="USER_ENTERED")

        except Exception as e:
            logger.warning(f"Error escribiendo estados honorarios en Q: {e}")

    def limpiar_y_escribir_info_col_q(self, logs_q: List[str]):
        """
        Limpia y escribe la columna Q en 'Informacion imagenes'.
        Usa lógica robusta compatible con gspread estándar.
        """
        if logs_q is None:
            return

        try:
            ws = self.sh.worksheet("Informacion imagenes")

            # Asegurar columna Q (17)
            if ws.col_count < 17:
                ws.add_cols(17 - ws.col_count)

            # Determinar cuántas filas escribir
            n_logs = len(logs_q)
            
            # Obtener altura total de la hoja para saber hasta dónde limpiar
            total_rows = ws.row_count
            
            # Construir datos para escribir (fila 2 en adelante)
            # Todo lo que exceda n_logs se llenará con ""
            
            # Estrategia: Escribir TODO el rango de la columna Q desde la fila 2 hasta el final de la hoja.
            # Si la hoja es muy larga (ej 1000 filas) y solo tenemos 5 logs, escribimos 5 valores y 995 vacíos.
            # Esto es una sola operación batch y garantiza limpieza.
            
            # Generar lista completa de valores para la columna Q
            # logs_q ya trae los valores para las filas con datos (según lectura previa)
            # Rellenamos con "" hasta el final de la hoja para limpiar lo viejo
            
            values = [[x] for x in logs_q]
            
            # Si hay más filas en la hoja que logs, agregar vacíos
            rows_to_clear = total_rows - 1 - n_logs # -1 por header
            if rows_to_clear > 0:
                 values.extend([[""] for _ in range(rows_to_clear)])

            # Asegurar que no excedemos el tamaño de la hoja (por si row_count cambió, improbable)
            values = values[:total_rows - 1] 

            if not values:
                return

            # Escribir en una sola operación
            range_name = f"Q2:Q{len(values) + 1}"
            ws.update(range_name, values, value_input_option="USER_ENTERED")
            logger.info(f"Columna Q actualizada: {n_logs} logs escritos, {len(values)} filas tocadas.")

        except Exception as e:
            logger.warning(f"Error escribiendo columna Q: {e}")

    # ─────────────────────────────────────────────────────────
    # Histórico
    # ─────────────────────────────────────────────────────────

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
