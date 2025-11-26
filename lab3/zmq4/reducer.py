import pickle
import sys
import time
import zmq
import constPipe

me = sys.argv[1]
if me == '1':
    address = f"tcp://{constPipe.SRC2}:{constPipe.PORT2}"
elif me == '2':
    address = f"tcp://{constPipe.SRC3}:{constPipe.PORT3}"
else:
    raise ValueError("Reducer ID must be '1' or '2'")

context = zmq.Context()
reducer = context.socket(zmq.PULL)
reducer.bind(address)

time.sleep(1)
print(f"Reducer {me} started")

counts = {}

while True:
    word = reducer.recv_string()
    print(f"Reducer {me} received '{word}'")
    if word == "STOP":
        print(f"Reducer {me} stopping")
        break

    counts[word] = counts.get(word, 0) + 1
    print(f"Reducer {me}: '{word}' -> {counts[word]}")
    
# Print final results
print(f"Reducer {me} final counts:")
for word, count in counts.items():
    print(f"{word}: {count}")