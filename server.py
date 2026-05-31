import asyncio
import websockets
import json
from datetime import datetime
from websockets import ConnectionClosed

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
        print(f"[{datetime.now()}] Player waiting for opponent...")

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
    async with websockets.serve(handler, "localhost", 8765):
        print("SkillForge server running at ws://localhost:8765")
        await asyncio.Future()

asyncio.run(main())