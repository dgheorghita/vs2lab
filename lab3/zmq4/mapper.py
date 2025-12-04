import pickle
import sys
import time
import hashlib
import string
import zmq

import constPipe

me = str(sys.argv[1])

context = zmq.Context()

# Splitter
receiver = context.socket(zmq.PULL)
splitter_addr = "tcp://" + constPipe.SRC1 + ":" + constPipe.PORT1
receiver.connect(splitter_addr)
print(f"Mapper {me} started. Connected to splitter at {splitter_addr}")

reducer_sockets = []

# Reducer 1
reducer1_addr = f"tcp://" + constPipe.SRC2 + ":" + constPipe.PORT2
sock1 = context.socket(zmq.PUSH)
sock1.connect(reducer1_addr)
reducer_sockets.append(sock1)
print(f"Mapper {me} started. Connected to reducer 1 at {reducer1_addr}")

# Reducer 2
reducer2_addr = f"tcp://" + constPipe.SRC3 + ":" + constPipe.PORT3
sock2 = context.socket(zmq.PUSH)
sock2.connect(reducer2_addr)
reducer_sockets.append(sock2)
print(f"Mapper {me} started. Connected to reducer 2 at {reducer2_addr}")

time.sleep(1)

while True:
    sentence = receiver.recv_string()
    if sentence == "STOP":
        for sock in reducer_sockets:
            sock.send_string("STOP")
        print(f"Mapper {me} stopping")
        break

    words = sentence.split()
    for word in words:
        word = word.strip().lower().strip(string.punctuation) #cleaning
        reducer_index = hash(word) % 2 
        #reducer_index = int(hashlib.md5(word.encode()).hexdigest(), 16) % 2
        reducer_sockets[reducer_index].send_string(word)
        print(f"Mapper {me} sent '{word}' to Reducer {reducer_index + 1}")