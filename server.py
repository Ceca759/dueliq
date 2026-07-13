import asyncio
import websockets
import json
from datetime import datetime
import os
import random
import urllib.request
import uuid

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ubrfipncygmigbgqhwal.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HOUSE_CUT = 0.15  # 15%
import random

WORD_LIST = [
    "apple","bridge","cloud","dance","eagle","flame","grape","house","island","jungle",
    "knife","lemon","magic","night","ocean","piano","queen","river","stone","tiger",
    "umbra","vault","wheat","xenon","yacht","zebra","amber","blaze","crisp","drift",
    "ember","frost","gloom","haste","ivory","joust","knack","lunar","marsh","noble",
    "olive","prism","quest","roast","scorn","thorn","ultra","vivid","waltz","exact",
    "youth","zonal","acorn","bloom","chess","depot","exile","fable","gaunt","haven",
    "input","joker","karma","latch","mirth","notch","orbit","perch","quirk","realm",
    "swift","truce","untie","visor","wrath","expel","yearn","zesty","aloft","brisk",
    "clasp","drone","elbow","fiend","gruel","hoist","inept","jamb","kinky","lodge",
    "manga","nadir","optic","plank","quill","raven","smirk","talon","usurp","vapor"
]

def get_word_list():
    words = WORD_LIST.copy()
    random.shuffle(words)
    return words
async def record_match(player1_id, player2_id, winner_id, game, entry_fee, payout):
    if not SUPABASE_KEY:
        print("[Supabase] No SUPABASE_KEY set, skipping match record")
        return
    try:
        payload = json.dumps({
            "player1_id": player1_id,
            "player2_id": player2_id,
            "winner_id": winner_id,
            "game": game,
            "entry_fee": entry_fee,
            "payout": payout
        }).encode()
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/matches",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer": "return=minimal"
            },
            method="POST"
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: urllib.request.urlopen(req, timeout=5))
        print(f"[Supabase] Match recorded: {game} winner={winner_id}")
    except Exception as e:
        print(f"[Supabase] Failed to record match: {e}")

# ── Lobby ──────────────────────────────────────────────────────────────────
# open_games: { game_id: { creator_ws, creator_id, creator_name, entry_fee, created_at } }
open_games = {}

# active_matches: { game_id: { p1_ws, p2_ws, p1_id, p2_id, p1_score, p2_score, entry_fee, moves: {} } }
active_matches = {}

# ws -> game_id (so we can clean up on disconnect)
ws_to_game = {}

connected_clients = set()

def is_open(ws):
    try:
        return ws.state.name == "OPEN"
    except:
        return False

def lobby_snapshot():
    """Return list of open games for the lobby board."""
    games = []
    for gid, g in open_games.items():
        games.append({
            "game_id": gid,
            "creator": g["creator_name"],
            "entry_fee": g["entry_fee"],
            "game_type": g.get("game_type", "rps"),
            "created_at": g["created_at"]
        })
    games.sort(key=lambda x: x["created_at"], reverse=True)
    return games
async def broadcast_lobby():
    """Send updated lobby to all connected clients."""
    if not connected_clients:
        return
    msg = json.dumps({"type": "lobby_update", "games": lobby_snapshot()})
    await asyncio.gather(*[c.send(msg) for c in connected_clients if is_open(c)], return_exceptions=True)

async def handler(websocket):
    connected_clients.add(websocket)
    print(f"[{datetime.now()}] Connected | Total: {len(connected_clients)}")

    # Send current lobby immediately on connect
    try:
        await websocket.send(json.dumps({"type": "lobby_update", "games": lobby_snapshot()}))
    except:
        pass

    try:
        async for message in websocket:
            data = json.loads(message)
            action = data.get("action")

            # ── Create a new game ──────────────────────────────────────
            if action == "create_game":
                entry_fee = float(data.get("entry_fee", 1))
                if entry_fee < 1:
                    await websocket.send(json.dumps({"type": "error", "msg": "Minimum entry fee is $1"}))
                    continue

                game_id = str(uuid.uuid4())[:8]
                open_games[game_id] = {
    "creator_ws": websocket,
    "creator_id": data.get("user_id"),
    "creator_name": data.get("username"),
    "entry_fee": entry_fee,
    "game_type": data.get("game_type", "rps"),
    "created_at": datetime.utcnow().isoformat()
}
                ws_to_game[websocket] = game_id

                await websocket.send(json.dumps({
                    "type": "game_created",
                    "game_id": game_id,
                    "entry_fee": entry_fee
                }))
                await broadcast_lobby()
                print(f"[Lobby] Game created: {game_id} by {data.get('username')} for ${entry_fee}")

            # ── Cancel a game (refund) ─────────────────────────────────
            elif action == "cancel_game":
                game_id = data.get("game_id")
                if game_id in open_games and open_games[game_id]["creator_ws"] == websocket:
                    del open_games[game_id]
                    ws_to_game.pop(websocket, None)
                    await websocket.send(json.dumps({"type": "game_cancelled", "game_id": game_id}))
                    await broadcast_lobby()
                    print(f"[Lobby] Game cancelled: {game_id}")

            # ── Join a game ────────────────────────────────────────────
            elif action == "join_game":
                game_id = data.get("game_id")
                if game_id not in open_games:
                    await websocket.send(json.dumps({"type": "error", "msg": "Game no longer available"}))
                    continue

                game = open_games.pop(game_id)
                creator_ws = game["creator_ws"]

                if not is_open(creator_ws):
                    await websocket.send(json.dumps({"type": "error", "msg": "Game creator disconnected"}))
                    await broadcast_lobby()
                    continue

                # Set up active match
                active_matches[game_id] = {
                    "p1_ws": creator_ws,
                    "p2_ws": websocket,
                    "p1_id": game["creator_id"],
                    "p2_id": data.get("user_id"),
                    "p1_name": game["creator_name"],
                    "p2_name": data.get("username"),
                    "p1_score": 0,
                    "p2_score": 0,
                    "entry_fee": game["entry_fee"],
                    "moves": {}
                }
                ws_to_game[creator_ws] = game_id
                ws_to_game[websocket] = game_id

               game_type = game.get("game_type", "rps")
words = get_word_list() if game_type == "typing" else []

active_matches[game_id]["game_type"] = game_type
active_matches[game_id]["words"] = words
active_matches[game_id]["p1_word_count"] = 0
active_matches[game_id]["p2_word_count"] = 0
active_matches[game_id]["timer_task"] = None

matched_msg_p1 = json.dumps({
    "type": "match_started",
    "game_id": game_id,
    "opponent": data.get("username"),
    "entry_fee": game["entry_fee"],
    "game_type": game_type,
    "words": words
})
matched_msg_p2 = json.dumps({
    "type": "match_started",
    "game_id": game_id,
    "opponent": game["creator_name"],
    "entry_fee": game["entry_fee"],
    "game_type": game_type,
    "words": words
})

                await creator_ws.send(matched_msg_p1)
                await websocket.send(matched_msg_p2)
                await broadcast_lobby()
                print(f"[Match] Started: {game_id} {game['creator_name']} vs {data.get('username')} for ${game['entry_fee']}")
# ── Typing word submitted ──────────────────────────────────────────────────
elif action == "typing_word":
    game_id = ws_to_game.get(websocket)
    if not game_id or game_id not in active_matches:
        continue
    match = active_matches[game_id]
    if match.get("game_type") != "typing":
        continue
    is_p1 = (websocket == match["p1_ws"])
    if is_p1:
        match["p1_word_count"] += 1
        count = match["p1_word_count"]
    else:
        match["p2_word_count"] += 1
        count = match["p2_word_count"]
    opp_ws = match["p2_ws"] if is_p1 else match["p1_ws"]
    if is_open(opp_ws):
        await opp_ws.send(json.dumps({
            "type": "opp_word_count",
            "count": match["p1_word_count"] if not is_p1 else match["p2_word_count"]
        }))

# ── Start typing timer ─────────────────────────────────────────────────────
elif action == "typing_start":
    game_id = ws_to_game.get(websocket)
    if not game_id or game_id not in active_matches:
        continue
    match = active_matches[game_id]
    if match.get("timer_started"):
        continue
    match["timer_started"] = True
    async def end_typing_match(gid):
        await asyncio.sleep(60)
        if gid not in active_matches:
            return
        m = active_matches.pop(gid)
        p1_count = m["p1_word_count"]
        p2_count = m["p2_word_count"]
        pot = m["entry_fee"] * 2
        payout = round(pot * (1 - HOUSE_CUT), 2)
        if p1_count > p2_count:
            winner_ws, loser_ws = m["p1_ws"], m["p2_ws"]
            winner_id, loser_id = m["p1_id"], m["p2_id"]
            w_score, l_score = p1_count, p2_count
        elif p2_count > p1_count:
            winner_ws, loser_ws = m["p2_ws"], m["p1_ws"]
            winner_id, loser_id = m["p2_id"], m["p1_id"]
            w_score, l_score = p2_count, p1_count
        else:
            # Tie — refund both (no house cut)
            refund = round(pot / 2, 2)
            for ws_ in [m["p1_ws"], m["p2_ws"]]:
                if is_open(ws_):
                    await ws_.send(json.dumps({"type": "typing_over", "won": None, "payout": refund, "your_score": p1_count, "opp_score": p2_count}))
            ws_to_game.pop(m["p1_ws"], None)
            ws_to_game.pop(m["p2_ws"], None)
            return
        if is_open(winner_ws):
            await winner_ws.send(json.dumps({"type": "typing_over", "won": True, "payout": payout, "your_score": w_score, "opp_score": l_score}))
        if is_open(loser_ws):
            await loser_ws.send(json.dumps({"type": "typing_over", "won": False, "payout": 0, "your_score": l_score, "opp_score": w_score}))
        await record_match(winner_id, loser_id, winner_id, "typing", m["entry_fee"], payout)
        ws_to_game.pop(m["p1_ws"], None)
        ws_to_game.pop(m["p2_ws"], None)
        print(f"[Typing] Over: {gid} winner={winner_id} payout=${payout}")
    asyncio.create_task(end_typing_match(game_id))
            # ── Send RPS move ──────────────────────────────────────────
            elif action == "move":
                game_id = ws_to_game.get(websocket)
                if not game_id or game_id not in active_matches:
                    continue

                match = active_matches[game_id]
                move = data.get("move")
                is_p1 = (websocket == match["p1_ws"])
                player_key = "p1" if is_p1 else "p2"
                opp_ws = match["p2_ws"] if is_p1 else match["p1_ws"]

                match["moves"][player_key] = move

                # Notify opponent that you've moved (without revealing the move)
                if is_open(opp_ws):
                    await opp_ws.send(json.dumps({"type": "opponent_moved"}))

                # Both players have moved — resolve the round
                if "p1" in match["moves"] and "p2" in match["moves"]:
    p1_move = match["moves"]["p1"]
    p2_move = match["moves"]["p2"]
    match["moves"] = {}
    for ws_ in [match["p1_ws"], match["p2_ws"]]:
        if is_open(ws_):
            await ws_.send(json.dumps({"type": "countdown"}))
    await asyncio.sleep(3)
    # Start 10 second move timer
    match["round_deadline"] = datetime.utcnow().timestamp() + 10
    for ws_ in [match["p1_ws"], match["p2_ws"]]:
        if is_open(ws_):
            await ws_.send(json.dumps({"type": "round_timer", "seconds": 10}))
    async def check_timeout(gid, deadline):
        await asyncio.sleep(10)
        if gid not in active_matches:
            return
        m = active_matches[gid]
        if datetime.utcnow().timestamp() < deadline + 0.5:
            return
        # whoever didn't move loses the round
        p1_moved = "p1" in m["moves"]
        p2_moved = "p2" in m["moves"]
        if p1_moved == p2_moved:
            return  # both moved or neither, ignore
        if not p1_moved:
            m["p2_score"] += 1
        else:
            m["p1_score"] += 1
        m["moves"] = {}
        for ws_, result, ys, os in [
            (m["p1_ws"], "lose" if not p1_moved else "win", m["p1_score"], m["p2_score"]),
            (m["p2_ws"], "win" if not p1_moved else "lose", m["p2_score"], m["p1_score"])
        ]:
            if is_open(ws_):
                await ws_.send(json.dumps({
                    "type": "round_result",
                    "your_move": "⏱️",
                    "opp_move": "⏱️",
                    "result": result,
                    "your_score": ys,
                    "opp_score": os,
                    "timeout": True
                }))
    asyncio.create_task(check_timeout(game_id, match["round_deadline"]))

                    result = get_rps_result(p1_move, p2_move)
                    if result == "p1":
                        match["p1_score"] += 1
                    elif result == "p2":
                        match["p2_score"] += 1

                    round_msg_p1 = json.dumps({
                        "type": "round_result",
                        "your_move": p1_move,
                        "opp_move": p2_move,
                        "result": "win" if result == "p1" else ("lose" if result == "p2" else "tie"),
                        "your_score": match["p1_score"],
                        "opp_score": match["p2_score"]
                    })
                    round_msg_p2 = json.dumps({
                        "type": "round_result",
                        "your_move": p2_move,
                        "opp_move": p1_move,
                        "result": "win" if result == "p2" else ("lose" if result == "p1" else "tie"),
                        "your_score": match["p2_score"],
                        "opp_score": match["p1_score"]
                    })

                    await match["p1_ws"].send(round_msg_p1)
                    if is_open(match["p2_ws"]):
                        await match["p2_ws"].send(round_msg_p2)

                    # Check if match is over (first to 3)
                    if match["p1_score"] >= 3 or match["p2_score"] >= 3:
                        winner_ws = match["p1_ws"] if match["p1_score"] >= 3 else match["p2_ws"]
                        loser_ws = match["p2_ws"] if match["p1_score"] >= 3 else match["p1_ws"]
                        winner_id = match["p1_id"] if match["p1_score"] >= 3 else match["p2_id"]
                        loser_id = match["p2_id"] if match["p1_score"] >= 3 else match["p1_id"]

                        pot = match["entry_fee"] * 2
                        payout = round(pot * (1 - HOUSE_CUT), 2)

                        await winner_ws.send(json.dumps({
                            "type": "match_over",
                            "won": True,
                            "payout": payout,
                            "your_score": match["p1_score"] if match["p1_score"] >= 3 else match["p2_score"],
                            "opp_score": match["p2_score"] if match["p1_score"] >= 3 else match["p1_score"]
                        }))
                        if is_open(loser_ws):
                            await loser_ws.send(json.dumps({
                                "type": "match_over",
                                "won": False,
                                "payout": 0,
                                "your_score": match["p2_score"] if match["p1_score"] >= 3 else match["p1_score"],
                                "opp_score": match["p1_score"] if match["p1_score"] >= 3 else match["p2_score"]
                            }))

                        await record_match(winner_id, loser_id, winner_id, "rps", match["entry_fee"], payout)

                        del active_matches[game_id]
                        ws_to_game.pop(match["p1_ws"], None)
                        ws_to_game.pop(match["p2_ws"], None)
                        print(f"[Match] Over: {game_id} winner={winner_id} payout=${payout}")

    finally:
        connected_clients.discard(websocket)

        # If they had an open game, remove it and refund (client-side handles refund on game_cancelled)
        game_id = ws_to_game.pop(websocket, None)
        if game_id:
            if game_id in open_games:
                # Creator disconnected before anyone joined
                del open_games[game_id]
                await broadcast_lobby()
            elif game_id in active_matches:
                # Player disconnected mid-match — opponent wins
                match = active_matches.pop(game_id)
                opp_ws = match["p2_ws"] if websocket == match["p1_ws"] else match["p1_ws"]
                opp_id = match["p2_id"] if websocket == match["p1_ws"] else match["p1_id"]
                my_id = match["p1_id"] if websocket == match["p1_ws"] else match["p2_id"]
                ws_to_game.pop(opp_ws, None)

                pot = match["entry_fee"] * 2
                payout = round(pot * (1 - HOUSE_CUT), 2)

                if is_open(opp_ws):
                    await opp_ws.send(json.dumps({
                        "type": "opponent_disconnected",
                        "payout": payout
                    }))
                await record_match(opp_id, my_id, opp_id, "rps", match["entry_fee"], payout)

        print(f"[{datetime.now()}] Disconnected | Total: {len(connected_clients)}")

def get_rps_result(p1, p2):
    if p1 == p2:
        return "tie"
    if (p1=="rock" and p2=="scissors") or (p1=="paper" and p2=="rock") or (p1=="scissors" and p2=="paper"):
        return "p1"
    return "p2"

async def main():
    port = int(os.environ.get("PORT", 8080))
    print(f"WebSocket starting on port {port}")
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"WebSocket running on port {port}")
        await asyncio.Future()

asyncio.run(main())