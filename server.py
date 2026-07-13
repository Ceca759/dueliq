import asyncio
import websockets
import json
from datetime import datetime
import os
import urllib.request
import uuid

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ubrfipncygmigbgqhwal.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HOUSE_CUT = 0.15  # 15%

# ── Phase 1 round timing ─────────────────────────────────────────────────────
COUNTDOWN_SECONDS = 3      # 3..2..1..GO on the client
MOVE_WINDOW_SECONDS = 10   # time to pick a move before auto-lose
RESULT_PAUSE_SECONDS = 2   # let the result animation show before next round
MAX_DOUBLE_TIMEOUTS = 2    # both players idle this many rounds in a row -> void + refund


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

# active_matches: { game_id: { p1_ws, p2_ws, p1_id, p2_id, p1_score, p2_score, entry_fee, moves, round_id, timer_task, no_move_streak } }
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
            "created_at": g["created_at"]
        })
    # Sort newest first
    games.sort(key=lambda x: x["created_at"], reverse=True)
    return games


async def broadcast_lobby():
    """Send updated lobby to all connected clients."""
    if not connected_clients:
        return
    msg = json.dumps({"type": "lobby_update", "games": lobby_snapshot()})
    await asyncio.gather(*[c.send(msg) for c in connected_clients if is_open(c)], return_exceptions=True)


async def send_both(match, msg_obj):
    """Send the same message object to both players in a match."""
    data = json.dumps(msg_obj)
    for ws_ in (match["p1_ws"], match["p2_ws"]):
        if is_open(ws_):
            try:
                await ws_.send(data)
            except:
                pass


def get_rps_result(p1, p2):
    if p1 == p2:
        return "tie"
    if (p1 == "rock" and p2 == "scissors") or (p1 == "paper" and p2 == "rock") or (p1 == "scissors" and p2 == "paper"):
        return "p1"
    return "p2"


# ── Round lifecycle (Phase 1: countdown + timer) ─────────────────────────────
async def start_round(game_id, delay=0.0):
    """Begin a new RPS round: countdown -> open a MOVE_WINDOW timer.

    Server-authoritative: the client plays its own 3-2-1-GO on `countdown`,
    then shows a matching visual timer on `round_timer`. Auto-lose is decided
    here, not on the client, so a frozen/spoofed client cannot dodge it.
    """
    if delay:
        await asyncio.sleep(delay)
    match = active_matches.get(game_id)
    if not match:
        return

    match["moves"] = {}
    # New round id so a stale timeout task from a prior round can't fire.
    match["round_id"] = match.get("round_id", 0) + 1
    round_id = match["round_id"]

    # 3-2-1-GO countdown (client animates this locally).
    await send_both(match, {"type": "countdown"})
    await asyncio.sleep(COUNTDOWN_SECONDS)
    if active_matches.get(game_id) is not match or match["round_id"] != round_id:
        return  # match ended or a newer round already started

    # Open the move window.
    match["deadline"] = datetime.utcnow().timestamp() + MOVE_WINDOW_SECONDS
    await send_both(match, {"type": "round_timer", "seconds": MOVE_WINDOW_SECONDS})
    match["timer_task"] = asyncio.create_task(check_timeout(game_id, round_id))


async def check_timeout(game_id, round_id):
    """Fires MOVE_WINDOW_SECONDS after the round opened. Resolves idle players."""
    await asyncio.sleep(MOVE_WINDOW_SECONDS)
    match = active_matches.get(game_id)
    if not match or match.get("round_id") != round_id:
        return  # round already resolved by real moves, or match gone

    p1_moved = "p1" in match["moves"]
    p2_moved = "p2" in match["moves"]

    # Both moved in time -> the move handler already resolved it.
    if p1_moved and p2_moved:
        return

    if not p1_moved and not p2_moved:
        # Nobody picked. Replay the round, but don't loop forever on two idlers.
        match["no_move_streak"] = match.get("no_move_streak", 0) + 1
        if match["no_move_streak"] >= MAX_DOUBLE_TIMEOUTS:
            await void_match_refund(game_id)
            return
        await send_both(match, {"type": "round_timeout_replay"})
        asyncio.create_task(start_round(game_id, delay=RESULT_PAUSE_SECONDS))
        return

    # Exactly one player moved -> the idle player auto-loses the round.
    match["no_move_streak"] = 0
    if p1_moved:
        match["p1_score"] += 1
        winner_key = "p1"
    else:
        match["p2_score"] += 1
        winner_key = "p2"
    match["moves"] = {}

    await send_both_round_result(match, timeout=True, winner_key=winner_key)
    await advance_after_round(game_id)


async def void_match_refund(game_id):
    """Both players idle too long -> refund entry fees, no house cut, end match."""
    match = active_matches.pop(game_id, None)
    if not match:
        return
    refund = round(match["entry_fee"], 2)
    await send_both(match, {"type": "match_voided", "refund": refund})
    ws_to_game.pop(match["p1_ws"], None)
    ws_to_game.pop(match["p2_ws"], None)
    print(f"[Match] Voided (both idle): {game_id} refund=${refund} each")


async def send_both_round_result(match, timeout=False, winner_key=None, p1_move=None, p2_move=None):
    """Send a round_result to each player from their own perspective."""
    if timeout:
        # Idle player has no move; show an hourglass on both sides.
        p1_move = p1_move or ("rock" if winner_key == "p1" else None)
        p2_move = p2_move or ("rock" if winner_key == "p2" else None)
        p1_result = "win" if winner_key == "p1" else "lose"
        p2_result = "win" if winner_key == "p2" else "lose"
    else:
        r = get_rps_result(p1_move, p2_move)
        p1_result = "win" if r == "p1" else ("lose" if r == "p2" else "tie")
        p2_result = "win" if r == "p2" else ("lose" if r == "p1" else "tie")

    if is_open(match["p1_ws"]):
        await match["p1_ws"].send(json.dumps({
            "type": "round_result",
            "your_move": p1_move,
            "opp_move": p2_move,
            "result": p1_result,
            "your_score": match["p1_score"],
            "opp_score": match["p2_score"],
            "timeout": timeout
        }))
    if is_open(match["p2_ws"]):
        await match["p2_ws"].send(json.dumps({
            "type": "round_result",
            "your_move": p2_move,
            "opp_move": p1_move,
            "result": p2_result,
            "your_score": match["p2_score"],
            "opp_score": match["p1_score"],
            "timeout": timeout
        }))


async def advance_after_round(game_id):
    """After a resolved round: end the match (first to 3) or start the next round."""
    match = active_matches.get(game_id)
    if not match:
        return

    if match["p1_score"] >= 3 or match["p2_score"] >= 3:
        p1_won = match["p1_score"] >= 3
        winner_ws = match["p1_ws"] if p1_won else match["p2_ws"]
        loser_ws = match["p2_ws"] if p1_won else match["p1_ws"]
        winner_id = match["p1_id"] if p1_won else match["p2_id"]
        loser_id = match["p2_id"] if p1_won else match["p1_id"]
        w_score = match["p1_score"] if p1_won else match["p2_score"]
        l_score = match["p2_score"] if p1_won else match["p1_score"]

        pot = match["entry_fee"] * 2
        payout = round(pot * (1 - HOUSE_CUT), 2)

        if is_open(winner_ws):
            await winner_ws.send(json.dumps({
                "type": "match_over", "won": True, "payout": payout,
                "your_score": w_score, "opp_score": l_score
            }))
        if is_open(loser_ws):
            await loser_ws.send(json.dumps({
                "type": "match_over", "won": False, "payout": 0,
                "your_score": l_score, "opp_score": w_score
            }))

        await record_match(winner_id, loser_id, winner_id, "rps", match["entry_fee"], payout)

        active_matches.pop(game_id, None)
        ws_to_game.pop(match["p1_ws"], None)
        ws_to_game.pop(match["p2_ws"], None)
        print(f"[Match] Over: {game_id} winner={winner_id} payout=${payout}")
    else:
        # Next round after a short pause so the result animation is visible.
        asyncio.create_task(start_round(game_id, delay=RESULT_PAUSE_SECONDS))


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
                    "moves": {},
                    "round_id": 0,
                    "timer_task": None,
                    "no_move_streak": 0
                }
                ws_to_game[creator_ws] = game_id
                ws_to_game[websocket] = game_id

                matched_msg_p1 = json.dumps({
                    "type": "match_started",
                    "game_id": game_id,
                    "opponent": data.get("username"),
                    "entry_fee": game["entry_fee"]
                })
                matched_msg_p2 = json.dumps({
                    "type": "match_started",
                    "game_id": game_id,
                    "opponent": game["creator_name"],
                    "entry_fee": game["entry_fee"]
                })

                await creator_ws.send(matched_msg_p1)
                await websocket.send(matched_msg_p2)
                await broadcast_lobby()
                print(f"[Match] Started: {game_id} {game['creator_name']} vs {data.get('username')} for ${game['entry_fee']}")

                # Kick off the first round (countdown -> timer).
                asyncio.create_task(start_round(game_id))

            # ── Send RPS move ──────────────────────────────────────────
            elif action == "move":
                game_id = ws_to_game.get(websocket)
                if not game_id or game_id not in active_matches:
                    continue

                match = active_matches[game_id]
                move = data.get("move")
                if move not in ("rock", "paper", "scissors"):
                    continue

                is_p1 = (websocket == match["p1_ws"])
                player_key = "p1" if is_p1 else "p2"
                opp_ws = match["p2_ws"] if is_p1 else match["p1_ws"]

                # Ignore duplicate moves and moves sent after the window closed.
                if player_key in match["moves"]:
                    continue
                if datetime.utcnow().timestamp() > match.get("deadline", 0) + 0.5:
                    continue

                match["moves"][player_key] = move

                # Notify opponent that you've moved (without revealing the move)
                if is_open(opp_ws):
                    await opp_ws.send(json.dumps({"type": "opponent_moved"}))

                # Both players have moved — cancel the timeout and resolve now.
                if "p1" in match["moves"] and "p2" in match["moves"]:
                    task = match.get("timer_task")
                    if task and not task.done():
                        task.cancel()
                    # Bump round_id so any in-flight timeout for this round no-ops.
                    match["round_id"] += 1

                    p1_move = match["moves"]["p1"]
                    p2_move = match["moves"]["p2"]
                    match["moves"] = {}
                    match["no_move_streak"] = 0

                    result = get_rps_result(p1_move, p2_move)
                    if result == "p1":
                        match["p1_score"] += 1
                    elif result == "p2":
                        match["p2_score"] += 1

                    await send_both_round_result(match, timeout=False, p1_move=p1_move, p2_move=p2_move)
                    await advance_after_round(game_id)

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
                task = match.get("timer_task")
                if task and not task.done():
                    task.cancel()
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


async def main():
    port = int(os.environ.get("PORT", 8080))
    print(f"WebSocket starting on port {port}")
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"WebSocket running on port {port}")
        await asyncio.Future()

asyncio.run(main())
