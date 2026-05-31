import asyncio
import websockets
import json
from datetime import datetime
import os
from websockets.asyncio.server import serve

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
        print(f"[{datetime.now()}] Player left | Total: {len(connected_clients)}")

async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"WebSocket server starting on port {port}")
    async with serve(handler, "0.0.0.0", port):
        print(f"WebSocket running on port {port}")
        await asyncio.Future()

asyncio.run(main())