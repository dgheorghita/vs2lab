import constPipe
import pickle
import sys
import time
import zmq

REDUCERS = [constPipe.PORT4, constPipe.PORT5]


class Mapper:
    def __init__(self, me):
        self.me = me

    def split_sentence(self, line):
        return line.split()

    def assign_reducer(self, word): 
        #entscheidet reducer
        return REDUCERS[hash(word) % len(REDUCERS)]

    def run(self):
        context = zmq.Context()

        # mit splitter verbinden
        pull_socket = context.socket(zmq.PULL)
        pull_socket.connect(f"tcp://{constPipe.SRC1}:{constPipe.PORT1}")

        # Verbindet reducer
        push_sockets = {}
        for r in REDUCERS:
            s = context.socket(zmq.PUSH)
            s.connect(f"tcp://127.0.0.1:{r}") # verbindet zu reducer port
            push_sockets[r] = s # speichert socket

        print(f"Mapper {self.me} gestartet")
        time.sleep(1)

        while True:
            sentence = pickle.loads(pull_socket.recv())
            words = self.split_sentence(sentence)
            print(f"Mapper {self.me} bekommt Satz: {sentence}")

            for w in words:
                r_port = self.assign_reducer(w)
                push_sockets[r_port].send(pickle.dumps(w)) #use old port
                print(f"Mapper {self.me} '{w}' -> Reducer {r_port}")


if __name__ == "__main__":
    me = sys.argv[1]
    mapper = Mapper(me)
    mapper.run()
