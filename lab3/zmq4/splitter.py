import pickle
import random
import sys
import time
import zmq
import constPipe

filename = "sentences.txt"

src = constPipe.SRC1
prt = constPipe.PORT1

context = zmq.Context()
splitter = context.socket(zmq.PUSH)

address = "tcp://" + src + ":" + prt
splitter.bind(address)

time.sleep(1)  # wait for mappers to connect
print(f"Splitter bound to {address}")

with open(filename, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            splitter.send_string(line)
            print(f"Sent: {line}")
    
for j in range(3):
    splitter.send_string("STOP")
    
print("Splitter finished sending sentences")