import sys
import zmq
import constWord


reducer_id = int(sys.argv[1])

context = zmq.Context()
socket = context.socket(zmq.PULL)

# Port abhängig von ID: 1 -> 5558, 2 -> 5559
prt = constWord.PORT2 if reducer_id == 1 else constWord.PORT3
socket.bind(f"tcp://*:{prt}")

print(f"Reducer {reducer_id} listening on port {prt} ...")

counts = {}

while True:
    word = socket.recv_string()
    word = word.strip()
    if not word:
        continue

    counts[word] = counts.get(word, 0) + 1
    print(f"[Reducer {reducer_id}] '{word}' -> {counts[word]}")
