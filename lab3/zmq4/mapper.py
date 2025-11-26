import sys
import zmq
import string
import constWord

def clean_word(w: str) -> str:
    # einfache Bereinigung: klein schreiben und Satzzeichen entfernen
    return w.strip().lower().strip(string.punctuation)


mapper_id = int(sys.argv[1])
adress1 = "tcp://" + constWord.SRC1 + ":" + constWord.PORT1  # 1st task src
address2 = "tcp://" + constWord.SRC2 + ":" + constWord.PORT2  # 1st task src
address3 = "tcp://" + constWord.SRC3 + ":" + constWord.PORT3  # 2nd task src

context = zmq.Context()

# PULL von Splitter
pull_socket = context.socket(zmq.PULL)
pull_socket.connect(adress1)

# PUSH zu Reducer 1 und 2
push_r1 = context.socket(zmq.PUSH)
push_r1.connect(address2)

push_r2 = context.socket(zmq.PUSH)
push_r2.connect(address3)

print(f"Mapper {mapper_id} connected (Splitter->5557, Reducer1->5558, Reducer2->5559)")

while True:
    sentence = pull_socket.recv_string()
    # Option: Stop-Signal behandeln, falls du später sauber beenden willst
    if sentence == "__END__":
        print(f"Mapper {mapper_id} received END signal.")
        continue

    words = sentence.split()
    for w in words:
         word = clean_word(w)
         if not word:
           continue

        # Hash-basierte Zuordnung (gerade, ungerade)
         if hash(word) % 2 == 0:
             push_r1.send_string(word)
         else:
             push_r2.send_string(word)

