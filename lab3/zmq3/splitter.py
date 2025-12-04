import pickle
import random
import sys
import time
import zmq
import constPipe


class Splitter:
    def __init__(self, text_name=None):
        self.sentences = []

        # Wenn Name gegeben -> Datei lesen
        if text_name:
            self.read_text(text_name)
        else:
            self.generate()

    def generate(self):
        words = ["car", "the", "drive", "speed", "fast", "corner"]
        for _ in range(10):
            sentence = " ".join(random.choices(words, k=4))
            self.sentences.append(sentence)

    def read_text(self, text_name):
        f = open(text_name, "r", encoding="utf-8")
        lines = f.readlines()
        f.close()

        self.sentences = []               # leere Liste 
        for line in lines:                
            clean_line = line.strip()     # Zeilenumbruch entfernen
            self.sentences.append(clean_line)  # zu self.sentences hinzufügen


    def run(self):
        context = zmq.Context()
        push_socket = context.socket(zmq.PUSH)
        push_socket.bind(f"tcp://{constPipe.SRC1}:{constPipe.PORT1}")
        print(f"[Splitter] gestartet auf Port {constPipe.PORT1}")

        time.sleep(2)  # warten auf Mapper

       
        for sentence in self.sentences:
            push_socket.send(pickle.dumps(sentence))
            print(f"Splitter sendet: {sentence}")
            time.sleep(0.1)

        print("Alle Sätze gesendet.")
        time.sleep(1)


if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        text_name = sys.argv[1]
    else:
        text_name = None


    splitter = Splitter(text_name)
    splitter.run()
