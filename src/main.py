"""
main.py — Entry point Flask para procesar-pagos
Rutas:
  GET  /                  → health check
  POST /procesar-pagos    → pipeline completo (cargar + honorarios + cuotas + histórico)
"""

import os
import time
import logging
from flask import Flask, request, jsonify, make_response

from src.pipelines.cargar_pagos import ejecutar_carga_pagos
from src.pipelines.pago_honorario import ejecutar_honorarios
from src.pipelines.cuotas_concepto import ejecutar_cuotas_concepto
from src.pipelines.historico import ejecutar_historico
from src import SheetsIO

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger("main")

app = Flask(__name__)

API_KEY = os.environ.get("API_KEY", "sheet_key")


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

    logger.info(f"[procesar-pagos] START spreadsheet={spreadsheet_id} by={created_by}")
    t0 = time.time()

    try:
        # Inicializar conexión a Sheets
        sheets = SheetsIO(spreadsheet_id)

        # 1. Cargar pagos en hojas mensuales
        logger.info("[procesar-pagos] Paso 1: cargar_pagos")
        resultado_carga = ejecutar_carga_pagos(sheets)

        # 2. Procesar honorarios
        logger.info("[procesar-pagos] Paso 2: honorarios")
        resultado_honorarios = ejecutar_honorarios(sheets)

        # 3. Recalcular cuotas y conceptos (reemplaza scheduler)
        logger.info("[procesar-pagos] Paso 3: cuotas_concepto")
        resultado_cuotas = ejecutar_cuotas_concepto(sheets)

        # 4. Guardar histórico
        logger.info("[procesar-pagos] Paso 4: historico")
        ejecutar_historico(sheets)

        elapsed = round(time.time() - t0, 2)
        logger.info(f"[procesar-pagos] DONE en {elapsed}s")

        return _json({
            "ok": True,
            "pagos_cargados": resultado_carga.get("cargados", 0),
            "duplicados": resultado_carga.get("duplicados", 0),
            "errores_carga": resultado_carga.get("errores", 0),
            "honorarios": resultado_honorarios.get("procesados", 0),
            "cuotas_actualizadas": resultado_cuotas.get("actualizadas", 0),
            "tiempo_seg": elapsed,
            "errores": resultado_carga.get("detalle_errores"),
        })

    except Exception as e:
        elapsed = round(time.time() - t0, 2)
        logger.exception(f"[procesar-pagos] ERROR: {e}")
        return _json({
            "ok": False,
            "error": str(e),
            "tiempo_seg": elapsed,
        }, 500)