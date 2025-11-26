import zmq
import time
import constWord
import random

INPUT_FILE = "input.txt"

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.bind("tcp://*: 5557")

print("Splitter binding on port 5557...")

# einfache Textquelle
try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
except FileNotFoundError:
        # fallback: ein paar Beispiel-Sätze
        lines = [
            "Was ist mein Sinn?",
            "Dass ich als Text in diesem Code stehe",
            "Oder dass du mich zählst",
            "und auf der Console ausgibst",
        ]

print(f"Splitter sends {len(lines)} lines ...")

for line in lines:
    socket.send_string(line)
    print(f"[Splitter] sent: {line}")
    time.sleep(0.01)  # kleine Pause, damit man die Ausgabe besser lesen kann

words = [
    "zero", "mq", "verteilt", "python", "map", "reduce",
    "task", "worker", "message", "pipeline", "parallel",
    "system", "daten", "zaehlen", "wort", "splitter",
    "mapper", "reducer"
]

NUM_RANDOM_SENTENCES = 20
print(f"Splitter generiert zusätzlich {NUM_RANDOM_SENTENCES} zufällige Sätze ...")

for i in range(NUM_RANDOM_SENTENCES):
    length = random.randint(3, 8)  # Anzahl Wörter im Satz
    sentence_words = random.choices(words, k=length)
    sentence = " ".join(sentence_words)

    socket.send_string(sentence)
    print(f"[Splitter] sent random: {sentence}")
    time.sleep(0.02)

    # Optional: End-Signal für Mapper
    # z.B. dreimal "__END__", wenn du drei Mapper hast
for _ in range(3):
     socket.send_string("__END__")

print("Splitter finished sending.")
