import pickle
import sys
import zmq
import time

single_words = {}


class Reducer:
    def __init__(self, me, port):
        self.me = me
        self.port = port

    
    def isolate_words(self, words):
        """Initialisiert Wörter mit 0."""
        global single_words
        for w in words:
            single_words[w] = 0

    def count_words(self, words):
        """Erhöht Zähler"""
        global single_words
        for w in words:
            if w in single_words:
                single_words[w] += 1

    def print_counts(self):
        """Gibt alle Wörter und ihre Zahlen aus."""
        for word, count in single_words.items():
            print(f"{word}: {count}")
    

    def run(self):
        context = zmq.Context()
        pull_socket = context.socket(zmq.PULL)

        
        pull_socket.bind(f"tcp://127.0.0.1:{self.port}")

        print(f"Reducer {self.me} gestartet")

        while True:
            word = pickle.loads(pull_socket.recv())
            single_words[word] = single_words.get(word, 0) + 1
            print(f"Reducer {self.me} {word}: {single_words[word]}")


if __name__ == "__main__":
    me = sys.argv[1]
    port = "50021" if me == "1" else "50022"

    reducer = Reducer(me, port)
    reducer.run()
