import json

def app(request):
    return (
        json.dumps({"ok": True, "msg": "alive"}),
        200,
        {"Content-Type": "application/json"},
    )
