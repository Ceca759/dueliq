import asyncio
import websockets
import json
from datetime import datetime
import os
import random

TRIVIA_QUESTIONS = [
    # Geography
    {"q": "What is the capital of France?", "options": ["London", "Berlin", "Paris", "Madrid"], "answer": "Paris"},
    {"q": "What is the largest ocean?", "options": ["Atlantic", "Indian", "Arctic", "Pacific"], "answer": "Pacific"},
    {"q": "What language is spoken in Brazil?", "options": ["Spanish", "Portuguese", "French", "English"], "answer": "Portuguese"},
    {"q": "What is the longest river in the world?", "options": ["Amazon", "Yangtze", "Mississippi", "Nile"], "answer": "Nile"},
    {"q": "Which country has the most natural lakes?", "options": ["USA", "Russia", "Canada", "Brazil"], "answer": "Canada"},
    {"q": "What is the smallest country in the world?", "options": ["Monaco", "San Marino", "Vatican City", "Liechtenstein"], "answer": "Vatican City"},
    {"q": "Which continent is the largest by area?", "options": ["Africa", "Asia", "North America", "Europe"], "answer": "Asia"},
    {"q": "What is the capital of Australia?", "options": ["Sydney", "Melbourne", "Brisbane", "Canberra"], "answer": "Canberra"},
    {"q": "Which country is home to the Amazon rainforest?", "options": ["Colombia", "Peru", "Brazil", "Venezuela"], "answer": "Brazil"},
    {"q": "What is the tallest mountain in the world?", "options": ["K2", "Kangchenjunga", "Mount Everest", "Lhotse"], "answer": "Mount Everest"},
    {"q": "Which desert is the largest in the world?", "options": ["Sahara", "Arabian", "Gobi", "Antarctic"], "answer": "Antarctic"},
    {"q": "What is the capital of Japan?", "options": ["Osaka", "Kyoto", "Tokyo", "Hiroshima"], "answer": "Tokyo"},
    {"q": "Which country has the longest coastline?", "options": ["Russia", "Australia", "Canada", "USA"], "answer": "Canada"},
    {"q": "What is the capital of Canada?", "options": ["Toronto", "Vancouver", "Montreal", "Ottawa"], "answer": "Ottawa"},
    {"q": "Which ocean is the smallest?", "options": ["Indian", "Arctic", "Atlantic", "Southern"], "answer": "Arctic"},

    # Science
    {"q": "What planet is known as the Red Planet?", "options": ["Venus", "Mars", "Jupiter", "Saturn"], "answer": "Mars"},
    {"q": "What gas do plants absorb?", "options": ["Oxygen", "Nitrogen", "Carbon Dioxide", "Hydrogen"], "answer": "Carbon Dioxide"},
    {"q": "How many bones are in the human body?", "options": ["196", "206", "216", "226"], "answer": "206"},
    {"q": "What is the chemical symbol for gold?", "options": ["Go", "Gd", "Au", "Ag"], "answer": "Au"},
    {"q": "What is H2O?", "options": ["Oxygen", "Hydrogen", "Water", "Salt"], "answer": "Water"},
    {"q": "Which planet is closest to the sun?", "options": ["Venus", "Earth", "Mars", "Mercury"], "answer": "Mercury"},
    {"q": "What is the speed of light (approx)?", "options": ["200,000 km/s", "300,000 km/s", "400,000 km/s", "500,000 km/s"], "answer": "300,000 km/s"},
    {"q": "What is the powerhouse of the cell?", "options": ["Nucleus", "Ribosome", "Mitochondria", "Chloroplast"], "answer": "Mitochondria"},
    {"q": "How many chromosomes do humans have?", "options": ["44", "46", "48", "50"], "answer": "46"},
    {"q": "What is the most abundant gas in Earth's atmosphere?", "options": ["Oxygen", "Carbon Dioxide", "Hydrogen", "Nitrogen"], "answer": "Nitrogen"},
    {"q": "What planet has the most moons?", "options": ["Jupiter", "Saturn", "Uranus", "Neptune"], "answer": "Saturn"},
    {"q": "What is the chemical symbol for iron?", "options": ["Ir", "Fe", "In", "Io"], "answer": "Fe"},
    {"q": "What organ produces insulin?", "options": ["Liver", "Kidney", "Pancreas", "Stomach"], "answer": "Pancreas"},
    {"q": "How many teeth does an adult human have?", "options": ["28", "30", "32", "34"], "answer": "32"},
    {"q": "What is the hardest natural substance on Earth?", "options": ["Gold", "Iron", "Diamond", "Quartz"], "answer": "Diamond"},
    {"q": "What force keeps planets in orbit around the sun?", "options": ["Magnetism", "Friction", "Gravity", "Electricity"], "answer": "Gravity"},
    {"q": "What is the atomic number of carbon?", "options": ["4", "6", "8", "12"], "answer": "6"},
    {"q": "What type of blood cells fight infection?", "options": ["Red blood cells", "Platelets", "White blood cells", "Plasma"], "answer": "White blood cells"},
    {"q": "How long does light from the sun take to reach Earth?", "options": ["2 minutes", "8 minutes", "20 minutes", "1 hour"], "answer": "8 minutes"},

    # Math
    {"q": "How many sides does a hexagon have?", "options": ["5", "6", "7", "8"], "answer": "6"},
    {"q": "What is 12 × 12?", "options": ["124", "144", "132", "148"], "answer": "144"},
    {"q": "What is the square root of 144?", "options": ["11", "12", "13", "14"], "answer": "12"},
    {"q": "What is 15% of 200?", "options": ["25", "30", "35", "40"], "answer": "30"},
    {"q": "What is 7 × 8?", "options": ["54", "56", "58", "62"], "answer": "56"},
    {"q": "What is the value of Pi (first 2 decimals)?", "options": ["3.12", "3.14", "3.16", "3.18"], "answer": "3.14"},
    {"q": "What is 2 to the power of 10?", "options": ["512", "1024", "2048", "256"], "answer": "1024"},
    {"q": "How many degrees are in a triangle?", "options": ["90", "180", "270", "360"], "answer": "180"},
    {"q": "What is 100 divided by 4?", "options": ["20", "25", "30", "40"], "answer": "25"},
    {"q": "What is the next prime number after 7?", "options": ["8", "9", "10", "11"], "answer": "11"},
    {"q": "What is 3 cubed?", "options": ["6", "9", "27", "81"], "answer": "27"},
    {"q": "How many sides does a pentagon have?", "options": ["4", "5", "6", "7"], "answer": "5"},

    # History
    {"q": "What year did World War 2 end?", "options": ["1943", "1944", "1945", "1946"], "answer": "1945"},
    {"q": "Who painted the Mona Lisa?", "options": ["Picasso", "Van Gogh", "Da Vinci", "Rembrandt"], "answer": "Da Vinci"},
    {"q": "Who wrote Romeo and Juliet?", "options": ["Dickens", "Shakespeare", "Tolkien", "Austen"], "answer": "Shakespeare"},
    {"q": "In what year did the Titanic sink?", "options": ["1910", "1912", "1914", "1916"], "answer": "1912"},
    {"q": "Who was the first man to walk on the moon?", "options": ["Buzz Aldrin", "Yuri Gagarin", "Neil Armstrong", "John Glenn"], "answer": "Neil Armstrong"},
    {"q": "In what year did World War 1 begin?", "options": ["1912", "1913", "1914", "1915"], "answer": "1914"},
    {"q": "Which empire was Julius Caesar part of?", "options": ["Greek", "Ottoman", "Roman", "Byzantine"], "answer": "Roman"},
    {"q": "Who was the first President of the United States?", "options": ["Thomas Jefferson", "John Adams", "Benjamin Franklin", "George Washington"], "answer": "George Washington"},
    {"q": "In what year did the Berlin Wall fall?", "options": ["1987", "1988", "1989", "1990"], "answer": "1989"},
    {"q": "Which country invented paper?", "options": ["Japan", "Egypt", "China", "India"], "answer": "China"},
    {"q": "What ancient wonder was located in Alexandria?", "options": ["Colossus", "Lighthouse", "Hanging Gardens", "Pyramids"], "answer": "Lighthouse"},
    {"q": "Who discovered penicillin?", "options": ["Marie Curie", "Louis Pasteur", "Alexander Fleming", "Jonas Salk"], "answer": "Alexander Fleming"},
    {"q": "In what year did man first land on the moon?", "options": ["1965", "1967", "1969", "1971"], "answer": "1969"},
    {"q": "Which country was Napoleon Bonaparte from?", "options": ["France", "Corsica (French)", "Italy", "Spain"], "answer": "Corsica (French)"},

    # Sports
    {"q": "How many players are on a basketball team?", "options": ["4", "5", "6", "7"], "answer": "5"},
    {"q": "How many players are on a football (soccer) team?", "options": ["9", "10", "11", "12"], "answer": "11"},
    {"q": "How many points is a touchdown worth in American football?", "options": ["4", "6", "7", "8"], "answer": "6"},
    {"q": "How many holes are in a standard golf round?", "options": ["9", "12", "18", "24"], "answer": "18"},
    {"q": "In tennis, what score comes after deuce?", "options": ["Match point", "Advantage", "Set point", "Tie-break"], "answer": "Advantage"},
    {"q": "How many rings are on the Olympic flag?", "options": ["4", "5", "6", "7"], "answer": "5"},
    {"q": "What sport is played at Wimbledon?", "options": ["Cricket", "Squash", "Tennis", "Badminton"], "answer": "Tennis"},
    {"q": "How many players are on a baseball team?", "options": ["7", "8", "9", "10"], "answer": "9"},
    {"q": "In which sport would you perform a slam dunk?", "options": ["Volleyball", "Basketball", "Handball", "Water polo"], "answer": "Basketball"},
    {"q": "How long is a marathon in kilometers?", "options": ["40km", "42.195km", "44km", "45km"], "answer": "42.195km"},

    # Pop Culture & General Knowledge
    {"q": "How many hours are in a day?", "options": ["12", "20", "24", "48"], "answer": "24"},
    {"q": "How many days are in a leap year?", "options": ["364", "365", "366", "367"], "answer": "366"},
    {"q": "What is the fastest land animal?", "options": ["Lion", "Horse", "Cheetah", "Leopard"], "answer": "Cheetah"},
    {"q": "How many continents are there?", "options": ["5", "6", "7", "8"], "answer": "7"},
    {"q": "How many strings does a standard guitar have?", "options": ["4", "5", "6", "7"], "answer": "6"},
    {"q": "What is the most spoken language in the world?", "options": ["English", "Spanish", "Hindi", "Mandarin Chinese"], "answer": "Mandarin Chinese"},
    {"q": "How many seconds are in one minute?", "options": ["30", "60", "90", "100"], "answer": "60"},
    {"q": "What color is the sun?", "options": ["Yellow", "Orange", "Red", "White"], "answer": "White"},
    {"q": "How many weeks are in a year?", "options": ["48", "50", "52", "54"], "answer": "52"},
    {"q": "What is the largest planet in our solar system?", "options": ["Saturn", "Neptune", "Jupiter", "Uranus"], "answer": "Jupiter"},
    {"q": "How many keys does a standard piano have?", "options": ["76", "80", "88", "92"], "answer": "88"},
    {"q": "What is the most common blood type?", "options": ["A+", "B+", "AB+", "O+"], "answer": "O+"},
    {"q": "How many players are on a volleyball team?", "options": ["4", "5", "6", "7"], "answer": "6"},
    {"q": "What is the square root of 256?", "options": ["14", "16", "18", "20"], "answer": "16"},
    {"q": "Which planet has rings around it?", "options": ["Mars", "Venus", "Saturn", "Neptune"], "answer": "Saturn"},
    {"q": "How many days are in February in a non-leap year?", "options": ["27", "28", "29", "30"], "answer": "28"},
    {"q": "What is the capital of Germany?", "options": ["Munich", "Hamburg", "Frankfurt", "Berlin"], "answer": "Berlin"},
    {"q": "How many ounces are in a pound?", "options": ["8", "12", "16", "20"], "answer": "16"},
    {"q": "What is the largest organ in the human body?", "options": ["Heart", "Liver", "Skin", "Lungs"], "answer": "Skin"},
    {"q": "How many letters are in the English alphabet?", "options": ["24", "25", "26", "27"], "answer": "26"},
    {"q": "What is the capital of Italy?", "options": ["Milan", "Venice", "Naples", "Rome"], "answer": "Rome"},
    {"q": "How many planets are in our solar system?", "options": ["7", "8", "9", "10"], "answer": "8"},
    {"q": "What is the boiling point of water in Celsius?", "options": ["90", "95", "100", "105"], "answer": "100"},
    {"q": "What currency does Japan use?", "options": ["Yuan", "Won", "Yen", "Baht"], "answer": "Yen"},
    {"q": "How many faces does a cube have?", "options": ["4", "5", "6", "8"], "answer": "6"},
]

connected_clients = set()

# RPS matchmaking
rps_waiting = None
rps_matches = {}

# Trivia matchmaking
trivia_waiting = None
trivia_matches = {}

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
    q = questions[0]
    msg = json.dumps({
        "type": "trivia_question",
        "question": q["q"],
        "options": q["options"],
        "index": 0,
        "total": 10,
        "scores": {"you": 0, "opp": 0}
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

    key = (id(ws), idx)
    if key in match["answered"]:
        return
    match["answered"].add(key)

    opp_match = trivia_matches.get(opponent)
    if opp_match:
        opp_match["answered"].add(key)

    correct = answer == q["answer"]

    if correct:
        # Right answer: +1 to answerer
        match["scores"][id(ws)] += 1
        if opp_match:
            opp_match["scores"][id(ws)] += 1
    else:
        # FIX: Wrong answer: +1 to opponent
        if opponent:
            match["scores"][id(opponent)] += 1
            if opp_match:
                opp_match["scores"][id(opponent)] += 1

    my_score = match["scores"][id(ws)]
    opp_score = match["scores"].get(id(opponent), 0)

    await ws.send(json.dumps({
        "type": "trivia_result",
        "correct": correct,
        "correct_answer": q["answer"],
        "my_score": my_score,
        "opp_score": opp_score,
        "question_idx": idx
    }))

    # Also notify opponent their score changed (they got a bonus point)
    if not correct and opponent and is_open(opponent):
        opp_my = match["scores"].get(id(opponent), 0)
        opp_opp = match["scores"].get(id(ws), 0)
        await opponent.send(json.dumps({
            "type": "trivia_score_update",
            "my_score": opp_my,
            "opp_score": opp_opp,
            "reason": "opponent_wrong"
        }))

    next_idx = idx + 1
    match["question_idx"] = next_idx
    if opp_match:
        opp_match["question_idx"] = next_idx

    if next_idx >= 10 or my_score >= 8 or opp_score >= 8:
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

        rps_opp = rps_matches.pop(websocket, None)
        if rps_opp:
            rps_matches.pop(rps_opp, None)
            if is_open(rps_opp):
                await rps_opp.send(json.dumps({"type": "opponent_left"}))
        if rps_waiting == websocket:
            rps_waiting = None

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