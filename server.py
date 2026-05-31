import asyncio
import websockets
import json
from datetime import datetime
import os
import random

TRIVIA_QUESTIONS = [
    {"q": "What is the capital of France?", "options": ["London", "Berlin", "Paris", "Madrid"], "answer": "Paris"},
    {"q": "How many sides does a hexagon have?", "options": ["5", "6", "7", "8"], "answer": "6"},
    {"q": "What planet is known as the Red Planet?", "options": ["Venus", "Mars", "Jupiter", "Saturn"], "answer": "Mars"},
    {"q": "What is 12 × 12?", "options": ["124", "144", "132", "148"], "answer": "144"},
    {"q": "Who painted the Mona Lisa?", "options": ["Picasso", "Van Gogh", "Da Vinci", "Rembrandt"], "answer": "Da Vinci"},
    {"q": "What is the largest ocean?", "options": ["Atlantic", "Indian", "Arctic", "Pacific"], "answer": "Pacific"},
    {"q": "How many bones are in the human body?", "options": ["196", "206", "216", "226"], "answer": "206"},
    {"q": "What gas do plants absorb?", "options": ["Oxygen", "Nitrogen", "Carbon Dioxide", "Hydrogen"], "answer": "Carbon Dioxide"},
    {"q": "What is the fastest land animal?", "options": ["Lion", "Horse", "Cheetah", "Leopard"], "answer": "Cheetah"},
    {"q": "How many continents are there?", "options": ["5", "6", "7", "8"], "answer": "7"},
    {"q": "What is the chemical symbol for gold?", "options": ["Go", "Gd", "Au", "Ag"], "answer": "Au"},
    {"q": "What year did World War 2 end?", "options": ["1943", "1944", "1945", "1946"], "answer": "1945"},
    {"q": "What is the square root of 144?", "options": ["11", "12", "13", "14"], "answer": "12"},
    {"q": "Which planet is closest to the sun?", "options": ["Venus", "Earth", "Mars", "Mercury"], "answer": "Mercury"},
    {"q": "What language is spoken in Brazil?", "options": ["Spanish", "Portuguese", "French", "English"], "answer": "Portuguese"},
    {"q": "How many players are on a basketball team?", "options": ["4", "5", "6", "7"], "answer": "5"},
    {"q": "What is H2O?", "options": ["Oxygen", "Hydrogen", "Water", "Salt"], "answer": "Water"},
    {"q": "Who wrote Romeo and Juliet?", "options": ["Dickens", "Shakespeare", "Tolkien", "Austen"], "answer": "Shakespeare"},
    {"q": "What is the longest river in the world?", "options": ["Amazon", "Yangtze", "Mississippi", "Nile"], "answer": "Nile"},
    {"q": "How many hours are in a day?", "options": ["12", "20", "24", "48"], "answer": "24"},
]

connected_clients = set()

# RPS matchmaking
rps_waiting = None
rps_matches = {}

# Trivia matchmaking
trivia_waiting = None
trivia_matches = {}  # ws -> {opponent, scores, question_idx, questions, answered}

def is_open(ws):
    try:
        return ws.state.name == "OPEN"
    except:
        return False

async def start_trivia_match(p1, p2):
    questions = random.sample(TRIVIA_QUESTIONS, 10)
    match_data = {
        "questions": questions,
        "question_idx": 0,
        "scores": {id(p1): 0, id(p2): 0},
        "answered": set(),
    }
    trivia_matches[p1] = {"opponent": p2, **match_data}
    trivia_matches[p2] = {"opponent": p1, **match_data}
    # Send first question
    q = questions[0]
    msg = json.dumps({
        "type": "trivia_question",
        "question": q["q"],
        "options": q["options"],
        "index": 0,
        "total": 10,
        "scores": {
            "you": 0,
            "opp": 0
        }
    })
    await p1.send(msg)
    await p2.send(msg)

async def handle_trivia_answer(ws, answer):
    match = trivia_matches.get(ws)
    if not match:
        return
    opponent = match["opponent"]
    questions = match["questions"]
    idx = match["question_idx"]
    q = questions[idx]

    # Prevent double answering same question
    key = (id(ws), idx)
    if key in match["answered"]:
        return
    match["answered"].add(key)

    # Also add to opponent's match data
    opp_match = trivia_matches.get(opponent)
    if opp_match:
        opp_match["answered"].add(key)

    correct = answer == q["answer"]
    if correct:
        match["scores"][id(ws)] += 1
        if opp_match:
            opp_match["scores"][id(ws)] += 1

    my_score = match["scores"][id(ws)]
    opp_score = match["scores"].get(id(opponent), 0)

    # Send result to answerer
    await ws.send(json.dumps({
        "type": "trivia_result",
        "correct": correct,
        "correct_answer": q["answer"],
        "my_score": my_score,
        "opp_score": opp_score,
        "question_idx": idx
    }))

    # Move to next question
    next_idx = idx + 1
    match["question_idx"] = next_idx
    if opp_match:
        opp_match["question_idx"] = next_idx

    if next_idx >= 10 or my_score >= 6 or opp_score >= 6:
        # End match
        opp_score_final = match["scores"].get(id(opponent), 0)
        await ws.send(json.dumps({
            "type": "trivia_end",
            "won": my_score > opp_score_final,
            "my_score": my_score,
            "opp_score": opp_score_final
        }))
        if opponent and is_open(opponent):
            opp_my = match["scores"].get(id(opponent), 0)
            opp_opp = match["scores"].get(id(ws), 0)
            await opponent.send(json.dumps({
                "type": "trivia_end",
                "won": opp_my > opp_opp,
                "my_score": opp_my,
                "opp_score": opp_opp
            }))
        trivia_matches.pop(ws, None)
        trivia_matches.pop(opponent, None)
    else:
        # Send next question to both
        next_q = questions[next_idx]
        msg = json.dumps({
            "type": "trivia_question",
            "question": next_q["q"],
            "options": next_q["options"],
            "index": next_idx,
            "total": 10,
            "scores": {"you": my_score, "opp": opp_score}
        })
        await ws.send(msg)
        if opponent and is_open(opponent):
            opp_my = match["scores"].get(id(opponent), 0)
            opp_opp = match["scores"].get(id(ws), 0)
            await opponent.send(json.dumps({
                "type": "trivia_question",
                "question": next_q["q"],
                "options": next_q["options"],
                "index": next_idx,
                "total": 10,
                "scores": {"you": opp_my, "opp": opp_opp}
            }))

async def handler(websocket):
    global rps_waiting, trivia_waiting
    connected_clients.add(websocket)
    print(f"[{datetime.now()}] New player | Total: {len(connected_clients)}")

    try:
        async for message in websocket:
            data = json.loads(message)
            game = data.get("game", "rps")

            if game == "rps":
                if data.get("action") == "join":
                    if rps_waiting and is_open(rps_waiting) and rps_waiting != websocket:
                        opponent = rps_waiting
                        rps_waiting = None
                        rps_matches[websocket] = opponent
                        rps_matches[opponent] = websocket
                        for p in [websocket, opponent]:
                            await p.send(json.dumps({"type": "matched"}))
                    else:
                        rps_waiting = websocket
                        await websocket.send(json.dumps({"type": "waiting"}))
                elif data.get("move"):
                    opponent = rps_matches.get(websocket)
                    if opponent and is_open(opponent):
                        await opponent.send(json.dumps({
                            "type": "opponent_move",
                            "move": data.get("move"),
                        }))

            elif game == "trivia":
                if data.get("action") == "join":
                    if trivia_waiting and is_open(trivia_waiting) and trivia_waiting != websocket:
                        opponent = trivia_waiting
                        trivia_waiting = None
                        await websocket.send(json.dumps({"type": "matched"}))
                        await opponent.send(json.dumps({"type": "matched"}))
                        await asyncio.sleep(0.5)
                        await start_trivia_match(websocket, opponent)
                    else:
                        trivia_waiting = websocket
                        await websocket.send(json.dumps({"type": "waiting"}))
                elif data.get("action") == "answer":
                    await handle_trivia_answer(websocket, data.get("answer"))

    finally:
        connected_clients.discard(websocket)

        # RPS cleanup
        rps_opp = rps_matches.pop(websocket, None)
        if rps_opp:
            rps_matches.pop(rps_opp, None)
            if is_open(rps_opp):
                await rps_opp.send(json.dumps({"type": "opponent_left"}))
        if rps_waiting == websocket:
            rps_waiting = None

        # Trivia cleanup
        trivia_match = trivia_matches.pop(websocket, None)
        if trivia_match:
            opp = trivia_match.get("opponent")
            if opp:
                trivia_matches.pop(opp, None)
                if is_open(opp):
                    await opp.send(json.dumps({"type": "opponent_left"}))
        if trivia_waiting == websocket:
            trivia_waiting = None

        print(f"[{datetime.now()}] Player left | Total: {len(connected_clients)}")

async def main():
    port = int(os.environ.get("PORT", 8080))
    print(f"WebSocket starting on port {port}")
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"WebSocket running on port {port}")
        await asyncio.Future()

asyncio.run(main())