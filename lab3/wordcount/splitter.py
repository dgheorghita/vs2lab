import pickle
import time
import zmq
import const_wordcount

def main():
    # Sample sentences for our word counting (you can also read from a file)
    sentences = [
        "The quick brown fox jumps over the lazy dog",
        "Hello world this is a test sentence",
        "Python is a powerful programming language",
        "ZeroMQ makes distributed computing easier",
        "MapReduce is used for big data processing",
        "The cat sat on the mat",
        "Distributed systems are complex but interesting",
        "This is another test sentence for our system",
        "The quick brown fox appears again",
        "Hello world we meet again",
        "Python programming is fun and educational",
        "The lazy dog sleeps all day long",
        "Big data requires distributed processing",
        "ZeroMQ provides excellent messaging patterns",
        "MapReduce simplifies parallel computing",
        "The mat is where the cat likes to sit",
        "Complex systems need careful design",
        "Test sentences help us verify functionality",
        "Quick brown animals are often foxes",
        "World peace is what we all want"
    ]
    
    # Create ZeroMQ context and socket
    context = zmq.Context()
    push_socket = context.socket(zmq.PUSH)
    
    # Bind to address where mappers will connect
    address = f"tcp://{const_wordcount.SPLITTER_HOST}:{const_wordcount.SPLITTER_PORT}"
    push_socket.bind(address)
    
    print("Splitter started, waiting for mappers to connect...")
    time.sleep(2)  # Give mappers time to connect
    
    print(f"Starting to send {len(sentences)} sentences to mappers...")
    
    # Send each sentence to mappers
    for i, sentence in enumerate(sentences):
        print(f"Sending sentence {i+1}: {sentence}")
        # Send the sentence as a pickled message
        push_socket.send(pickle.dumps(sentence))
        time.sleep(0.1)  # Small delay to see the distribution
    
    print("All sentences sent!")
    
    # Send termination signals (one for each mapper)
    print("Sending termination signals...")
    for i in range(3):  # We have 3 mappers
        push_socket.send(pickle.dumps("TERMINATE"))
    
    push_socket.close()
    context.term()

if __name__ == "__main__":
    main()