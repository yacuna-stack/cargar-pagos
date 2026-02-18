"""
config.py — Constantes y configuración central
"""

import os

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")

# ─── Headers de hojas mensuales (25 columnas) ───
HEADERS_MES = [
    "DNI",              # A  (0)
    "Nombre",           # B  (1)
    "Fecha",            # C  (2)
    "Importe",          # D  (3)
    "Concepto",         # E  (4)
    "Tipo de Pago",     # F  (5)
    "Nro de Cuota",     # G  (6)
    "Total Cuotas",     # H  (7)
    "Cartera",          # I  (8)
    "Cartera Cta.",     # J  (9)
    "Producto Cta.",    # K  (10)
    "Operador",         # L  (11)
    "Entidad",          # M  (12)
    "Cta. Destino",     # N  (13)
    "Observaciones",    # O  (14)
    "Transferido",      # P  (15)
    "Nº Día",           # Q  (16)
    "ID",               # R  (17)
    "Tipo Doc",         # S  (18)
    "NUMEDOCU",         # T  (19)
    "FECHPAGO",         # U  (20)
    "MONTPAGO",         # V  (21)
    "TPO_ORIG",         # W  (22)
    "MANGO",            # X  (23)
    "Honorario",        # Y  (24)
]

# ─── Meses ───
MESES_ES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]
MESES_ABREV = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]

# ─── Mapping canal de pago → código ───
CANAL_TO_CODE = {
    "banco": 2,
    "estudio": 1,
    "ctacomafi": 25,
    "ctacreditia": 24,
    "rapipago": 5,
    "ofcreditia": 23,
    "mercpago": 10,
    "pagomiscuentas": 26,
    "ctaexi": 27,
    "ctarda": 31,
    "pagofacil": 4,
    "efectivosi": 32,
    "ctaefectivosi": 33,
    "mins": 66,
    "Cta Creditia": 24, 
    "Cta. Mins": 34,
    "cta Efetivo Si": 33,
    "Cta. Creditia": 24,
    "Cta. RDA": 31,
    "Banco ": 2,
}

# ─── Entidades conocidas ───
ENTIDADES_CONOCIDAS = [
    "COMAFI", "CREDITIA", "EXI", "WENANCE", "CEIBO",
    "GALICIA 2º", "EFECTIVO SI", "RDA", "COLUMBIA", "MINS",
]