"""
sheets_io.py — Lectura y escritura centralizada de Google Sheets
Usa gspread para operaciones batch eficientes.

Optimizaciones vs versión anterior:
- Escritura en chunks para evitar OOM y timeouts de la API
- Retry automático con backoff exponencial en errores transitorios
- Logging mejorado con tiempos de ejecución
"""

import logging
import time
from typing import List, Any, Dict, Tuple, Optional

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
import google.auth
from google.auth.transport.requests import Request as AuthRequest

from src.utils.config import HEADERS_MES

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ─── Configuración de batching y retry ───────────────────
BATCH_SIZE = 500          # filas por escritura
MAX_RETRIES = 3           # reintentos en errores transitorios
RETRY_BASE_DELAY = 2      # segundos base entre reintentos
RETRY_CODES = {429, 500, 502, 503}  # HTTP codes que merecen retry

# Credenciales globales
_creds, _ = google.auth.default(scopes=SCOPES)
_auth_req = AuthRequest()


def _refresh():
    if not _creds.valid:
        _creds.refresh(_auth_req)


def _retry_api_call(func, *args, **kwargs):
    """
    Ejecuta una llamada a la API con retry + backoff exponencial.
    Reintenta en errores 429 (rate limit), 500, 502, 503.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            last_err = e
            code = e.response.status_code if hasattr(e, "response") else 0
            if code not in RETRY_CODES or attempt == MAX_RETRIES:
                raise
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(
                f"API error {code} en intento {attempt}/{MAX_RETRIES}, "
                f"reintentando en {delay}s: {e}"
            )
            time.sleep(delay)
        except Exception as e:
            # ConnectionError, timeout, etc.
            last_err = e
            if attempt == MAX_RETRIES:
                raise
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(
                f"Error transitorio en intento {attempt}/{MAX_RETRIES}, "
                f"reintentando en {delay}s: {e}"
            )
            time.sleep(delay)
    raise last_err


def _write_column_batched(ws: gspread.Worksheet, col_letter: str,
                          values: List[Any], start_row: int = 2):
    """
    Escribe una lista de valores en una columna, dividiendo en chunks
    de BATCH_SIZE para evitar OOM y timeouts de la API de Sheets.
    """
    if not values:
        return

    total = len(values)
    for offset in range(0, total, BATCH_SIZE):
        chunk = values[offset: offset + BATCH_SIZE]
        row_start = start_row + offset
        row_end = row_start + len(chunk) - 1
        rng = f"{col_letter}{row_start}:{col_letter}{row_end}"
        payload = [[v] for v in chunk]

        _retry_api_call(
            ws.update, rng, payload, value_input_option="USER_ENTERED"
        )

        if total > BATCH_SIZE:
            logger.debug(
                f"  Batch {col_letter}: filas {row_start}-{row_end} "
                f"({len(chunk)}/{total})"
            )


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
        """Obtiene worksheet por nombre."""
        return self.sh.worksheet(nombre)

    def _ensure_col_count(self, ws: gspread.Worksheet, min_cols: int):
        """Asegura que la hoja tenga al menos min_cols columnas."""
        if ws.col_count < min_cols:
            ws.add_cols(min_cols - ws.col_count)

    # ─────────────────────────────────────────────────────────
    # Lectura
    # ─────────────────────────────────────────────────────────

    def _leer_por_rangos(self, ws: gspread.Worksheet,
                         batch_size: int = BATCH_SIZE) -> List[List[Any]]:
        """
        Lee una hoja en bloques de filas para evitar OOM en hojas grandes.
        Retorna todas las filas incluyendo header.
        """
        total_rows = ws.row_count
        total_cols = ws.col_count
        if total_rows == 0 or total_cols == 0:
            return []

        last_col = self._col_letter(total_cols)
        all_data = []

        for start in range(1, total_rows + 1, batch_size):
            end = min(start + batch_size - 1, total_rows)
            rng = f"A{start}:{last_col}{end}"
            chunk = _retry_api_call(ws.get, rng)
            if chunk:
                all_data.extend(chunk)
            else:
                # Hoja termina antes de row_count (filas vacías al final)
                break

        return all_data

    def leer_hoja(self, nombre: str, batched: bool = False) -> List[List[Any]]:
        """
        Lee todos los datos de una hoja (incluye header).
        Si batched=True, lee en bloques (para hojas grandes >5K filas).
        """
        try:
            ws = self.sh.worksheet(nombre)
            if batched:
                return self._leer_por_rangos(ws)
            return _retry_api_call(ws.get_all_values)
        except WorksheetNotFound:
            logger.warning(f"Hoja '{nombre}' no encontrada")
            return []

    def leer_hoja_sin_header(self, nombre: str,
                             batched: bool = False) -> List[List[Any]]:
        """Lee datos sin la fila de header."""
        data = self.leer_hoja(nombre, batched=batched)
        return data[1:] if len(data) > 1 else []

    def leer_info_imagenes(self) -> List[List[Any]]:
        """Lee 'Informacion imagenes' sin header."""
        return self.leer_hoja_sin_header("Informacion imagenes")

    def leer_pro(self) -> Tuple[List[Any], List[List[Any]]]:
        """Lee hoja 'Pro'. Retorna (header, data_rows). Usa lectura batched."""
        data = self.leer_hoja("Pro", batched=True)
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
        except WorksheetNotFound:
            ws = self.sh.add_worksheet(
                title=nombre, rows=1000, cols=len(HEADERS_MES)
            )
            _retry_api_call(
                ws.append_row, HEADERS_MES, value_input_option="USER_ENTERED"
            )
            logger.info(f"Creada hoja '{nombre}'")

        self._hoja_cache[nombre] = ws
        return ws

    def leer_hoja_mes(self, nombre: str) -> List[List[Any]]:
        """Lee datos de una hoja mensual (con header)."""
        ws = self.obtener_o_crear_hoja_mes(nombre)
        return _retry_api_call(ws.get_all_values)

    def escribir_filas_mes(self, nombre: str, filas: List[List[Any]]):
        """Escribe filas al final de una hoja mensual."""
        if not filas:
            return
        ws = self.obtener_o_crear_hoja_mes(nombre)

        # Escribir en chunks para hojas grandes
        for offset in range(0, len(filas), BATCH_SIZE):
            chunk = filas[offset: offset + BATCH_SIZE]
            _retry_api_call(
                ws.append_rows, chunk, value_input_option="USER_ENTERED"
            )

        logger.info(f"Escritas {len(filas)} filas en '{nombre}'")

    def actualizar_columna_mes(self, nombre: str, col_idx: int,
                               valores: List[Any]):
        """Actualiza una columna completa (desde fila 2) en hoja mensual."""
        if not valores:
            return
        ws = self.obtener_o_crear_hoja_mes(nombre)
        col_letter = self._col_letter(col_idx + 1)

        t0 = time.time()
        _write_column_batched(ws, col_letter, valores)
        logger.info(
            f"Columna {col_letter} actualizada en '{nombre}': "
            f"{len(valores)} filas en {time.time() - t0:.1f}s"
        )

    def actualizar_dos_columnas_mes(self, nombre: str,
                                     col_e_vals: List[Any],
                                     col_g_vals: List[Any]):
        """Actualiza columnas E (concepto) y G (cuota) de la hoja mensual."""
        ws = self.obtener_o_crear_hoja_mes(nombre)
        total = len(col_e_vals)
        if total == 0:
            return

        t0 = time.time()
        _write_column_batched(ws, "E", col_e_vals)
        _write_column_batched(ws, "G", col_g_vals)
        logger.info(
            f"Columnas E+G actualizadas en '{nombre}': "
            f"{total} filas en {time.time() - t0:.1f}s"
        )

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
            self._ensure_col_count(ws, col)
            col_letter = self._col_letter(col)
            _write_column_batched(ws, col_letter, estados)
        except Exception as e:
            logger.warning(f"Error escribiendo estados: {e}")

    def escribir_estado_info_imagenes_col_q(self, marcas_q: Dict[int, str]):
        """
        Escribe estados SOLO en las filas indicadas en 'Informacion imagenes',
        columna Q.

        marcas_q: dict {idx_0based: "texto"}, fila real = idx + 2.
        No pisa otras filas.
        """
        if not marcas_q:
            return

        try:
            ws = self.sh.worksheet("Informacion imagenes")
            self._ensure_col_count(ws, 17)

            # Agrupar en batches para batch_update
            updates = [
                {"range": f"Q{int(idx) + 2}", "values": [[str(txt)]]}
                for idx, txt in marcas_q.items()
            ]

            for offset in range(0, len(updates), BATCH_SIZE):
                chunk = updates[offset: offset + BATCH_SIZE]
                _retry_api_call(
                    ws.batch_update, chunk, value_input_option="USER_ENTERED"
                )

        except Exception as e:
            logger.warning(f"Error escribiendo estados honorarios en Q: {e}")

    def limpiar_y_escribir_info_col_q(self, logs_q: List[str]):
        """
        Limpia y escribe la columna Q en 'Informacion imagenes'.
        """
        if logs_q is None:
            return

        try:
            ws = self.sh.worksheet("Informacion imagenes")
            self._ensure_col_count(ws, 17)

            total_rows = ws.row_count
            data_rows = total_rows - 1  # sin header

            # Construir valores: logs reales + vacíos para limpiar el resto
            values = list(logs_q)
            if len(values) < data_rows:
                values.extend([""] * (data_rows - len(values)))
            values = values[:data_rows]

            if not values:
                return

            t0 = time.time()
            _write_column_batched(ws, "Q", values)
            logger.info(
                f"Columna Q actualizada: {len(logs_q)} logs, "
                f"{len(values)} filas tocadas en {time.time() - t0:.1f}s"
            )

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
        except WorksheetNotFound:
            ws = self.sh.add_worksheet(
                title="Historico", rows=1000, cols=len(header)
            )
            _retry_api_call(
                ws.append_row, header, value_input_option="RAW"
            )
            logger.info("Creada hoja 'Historico'")

        for offset in range(0, len(filas), BATCH_SIZE):
            chunk = filas[offset: offset + BATCH_SIZE]
            _retry_api_call(
                ws.append_rows, chunk, value_input_option="RAW"
            )

        logger.info(f"Copiadas {len(filas)} filas a 'Historico'")
