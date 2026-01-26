import os
from flask import Flask, jsonify
from googleapiclient.discovery import build
from google.auth import default

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_NAME = "Informacion imagenes"

app = Flask(__name__)

def get_sheets_service():
    creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

@app.get("/")
def health():
    return jsonify(ok=True, service="procesar-pagos")

@app.post("/sheets-test")
def sheets_test():
    if not SPREADSHEET_ID:
        return jsonify(ok=False, error="Missing SPREADSHEET_ID env var"), 500

    svc = get_sheets_service()
    sheet = svc.spreadsheets()

    # buscar primera fila vacía en A
    res = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:A"
    ).execute()

    values = res.get("values", [])
    row = len(values) + 1

    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A{row}",
        valueInputOption="RAW",
        body={"values": [["esto es una prueba de conexion"]]}
    ).execute()

    return jsonify(ok=True, written=f"{SHEET_NAME}!A{row}")
