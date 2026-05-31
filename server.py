import asyncio
import websockets
import json
from datetime import datetime
from flask import Flask, send_from_directory
import threading
import os

flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return send_from_directory(".", "skillforge.html")

connected_clients = set()
waiting_player = None
matches = {}

def is_open(ws):
    return ws.state.name == "OPEN"

async def handler(websocket):
    global waiting_player
    connected_clients.add(websocket)
    print(f"[{datetime.now()}] New player | Total: {len(connected_clients)}")

    if waiting_player and is_open(waiting_player):
        opponent = waiting_player
        waiting_player = None
        matches[websocket] = opponent
        matches[opponent] = websocket
        for p in [websocket, opponent]:
            await p.send(json.dumps({"type": "matched"}))
        print(f"[{datetime.now()}] Match created!")
    else:
        waiting_player = websocket
        await websocket.send(json.dumps({"type": "waiting"}))

    try:
        async for message in websocket:
            data = json.loads(message)
            opponent = matches.get(websocket)
            if opponent and is_open(opponent):
                await opponent.send(json.dumps({
                    "type": "opponent_move",
                    "move": data.get("move"),
                    "timestamp": datetime.now().isoformat()
                }))
    finally:
        connected_clients.discard(websocket)
        opponent = matches.pop(websocket, None)
        if opponent:
            matches.pop(opponent, None)
            if is_open(opponent):
                await opponent.send(json.dumps({"type": "opponent_left"}))
        if waiting_player == websocket:
            waiting_player = None

async def main():
    port = int(os.environ.get("PORT", 5000))
    ws_port = int(os.environ.get("WS_PORT", 8765))

    flask_thread = threading.Thread(
        target=lambda: flask_app.run(host="0.0.0.0", port=port)
    )
    flask_thread.daemon = True
    flask_thread.start()
    print(f"Flask running on port {port}")

    async with websockets.serve(handler, "0.0.0.0", ws_port):
        print(f"WebSocket running on port {ws_port}")
        await asyncio.Future()

asyncio.run(main())