"""
main.py — Entry point Flask para procesar-pagos
Rutas:
  GET  /                  → health check
  POST /procesar-pagos    → pipeline completo (cargar + honorarios + cuotas + histórico)
"""

import gc
import os
import time
import logging
import resource

from flask import Flask, request, jsonify, make_response

from src.pipelines.cargar_pagos import ejecutar_carga_pagos
from src.pipelines.pago_honorario import ejecutar_honorarios
from src.pipelines.cuotas_concepto import ejecutar_cuotas_concepto
from src.pipelines.historico import ejecutar_historico
from src.utils.sheets_io import SheetsIO

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger("main")

app = Flask(__name__)

API_KEY = os.environ.get("API_KEY", "sheet_key")


# ─── Helpers ───

def _require_key():
    if not API_KEY:
        return False, "API_KEY no configurada"
    if request.headers.get("X-API-KEY") != API_KEY:
        return False, "unauthorized"
    return True, None


def _json(payload, status=200):
    resp = make_response(jsonify(payload), status)
    resp.headers["Content-Type"] = "application/json"
    return resp


def _mem_mb() -> float:
    """Retorna el uso de memoria RSS actual en MB."""
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / 1024  # Linux reporta en KB
    except Exception:
        return 0.0


def _liberar_memoria(sheets: SheetsIO, paso: str):
    """Invalida cache de Pro y fuerza garbage collection entre pasos."""
    sheets.invalidar_cache_pro()
    gc.collect()
    logger.info(f"  [{paso}] memoria liberada — RSS: {_mem_mb():.0f} MB")


# ─── Health ───

@app.get("/")
def health():
    return _json({"ok": True, "service": "procesar-pagos"})


# ─── Pipeline completo ───

@app.post("/procesar-pagos")
def procesar_pagos():
    ok, err = _require_key()
    if not ok:
        return _json({"ok": False, "error": err}, 401)

    body = request.get_json(silent=True) or {}
    spreadsheet_id = body.get("spreadsheet_id")
    created_by = body.get("created_by", "unknown")

    if not spreadsheet_id:
        return _json({"ok": False, "error": "spreadsheet_id requerido"}, 400)

    logger.info(
        f"[procesar-pagos] START spreadsheet={spreadsheet_id} "
        f"by={created_by} — RSS: {_mem_mb():.0f} MB"
    )
    t0 = time.time()
    tiempos = {}

    try:
        sheets = SheetsIO(spreadsheet_id)

        # 1. Cargar pagos en hojas mensuales
        logger.info("[procesar-pagos] Paso 1: cargar_pagos")
        t1 = time.time()
        resultado_carga = ejecutar_carga_pagos(sheets)
        tiempos["cargar_pagos"] = round(time.time() - t1, 2)
        _liberar_memoria(sheets, "cargar_pagos")

        # 2. Procesar honorarios
        logger.info("[procesar-pagos] Paso 2: honorarios")
        t2 = time.time()
        resultado_honorarios = ejecutar_honorarios(sheets)
        tiempos["honorarios"] = round(time.time() - t2, 2)
        _liberar_memoria(sheets, "honorarios")

        # 3. Recalcular cuotas y conceptos
        logger.info("[procesar-pagos] Paso 3: cuotas_concepto")
        t3 = time.time()
        resultado_cuotas = ejecutar_cuotas_concepto(sheets)
        tiempos["cuotas_concepto"] = round(time.time() - t3, 2)
        _liberar_memoria(sheets, "cuotas_concepto")

        # 4. Guardar histórico
        logger.info("[procesar-pagos] Paso 4: historico")
        t4 = time.time()
        ejecutar_historico(sheets)
        tiempos["historico"] = round(time.time() - t4, 2)

        elapsed = round(time.time() - t0, 2)
        logger.info(
            f"[procesar-pagos] DONE en {elapsed}s — "
            f"pasos: {tiempos} — RSS: {_mem_mb():.0f} MB"
        )

        return _json({
            "ok": True,
            "pagos_cargados": resultado_carga.get("cargados", 0),
            "duplicados": resultado_carga.get("duplicados", 0),
            "errores_carga": resultado_carga.get("errores", 0),
            "honorarios": resultado_honorarios.get("procesados", 0),
            "cuotas_actualizadas": resultado_cuotas.get("actualizadas", 0),
            "tiempo_seg": elapsed,
            "tiempos_pasos": tiempos,
            "errores": resultado_carga.get("detalle_errores"),
        })

    except Exception as e:
        elapsed = round(time.time() - t0, 2)
        logger.exception(
            f"[procesar-pagos] ERROR: {e} — "
            f"elapsed={elapsed}s — RSS: {_mem_mb():.0f} MB"
        )
        return _json({
            "ok": False,
            "error": str(e),
            "tiempo_seg": elapsed,
        }, 500)
