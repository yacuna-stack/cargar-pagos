import json
import os

from googleapiclient.discovery import build
from google.auth import default


SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_NAME = "Informacion imagenes"
TEST_TEXT = "esto es una prueba de conexion"


def get_sheets_service():
    creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def write_test_message():
    if not SPREADSHEET_ID:
        raise Exception("SPREADSHEET_ID no configurado")

    service = get_sheets_service()

    sheet = service.spreadsheets()

    # Leer columna A completa
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:A"
    ).execute()

    values = result.get("values", [])

    # Primera fila vacía (1-based)
    row = len(values) + 1

    target_range = f"{SHEET_NAME}!A{row}"

    body = {
        "values": [[TEST_TEXT]]
    }

    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=target_range,
        valueInputOption="RAW",
        body=body
    ).execute()

    return row


def app(request):
    path = request.path or "/"

    try:
        if path == "/":
            return (
                json.dumps({"ok": True, "service": "alive"}),
                200,
                {"Content-Type": "application/json"},
            )

        if path == "/sheets-test":
            row = write_test_message()

            return (
                json.dumps({
                    "ok": True,
                    "written_row": row,
                    "sheet": SHEET_NAME
                }),
                200,
                {"Content-Type": "application/json"},
            )

        return (
            json.dumps({"ok": False, "error": "not_found"}),
            404,
            {"Content-Type": "application/json"},
        )

    except Exception as e:
        return (
            json.dumps({"ok": False, "error": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )
