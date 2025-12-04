import pickle
import sys
import time
import re
import zmq
import const_wordcount

def word_to_reducer(word):
    """
    Determine which reducer should handle this word.
    We'll use a simple hash-based approach: words starting with A-M go to reducer 0,
    words starting with N-Z go to reducer 1.
    """
    first_char = word.lower()[0] if word else 'a'
    if 'a' <= first_char <= 'm':
        return 0
    else:
        return 1

def main():
    if len(sys.argv) != 2:
        print("Usage: python mapper.py <mapper_id>")
        sys.exit(1)
    
    mapper_id = sys.argv[1]
    
    # Create ZeroMQ context
    context = zmq.Context()
    
    # Create PULL socket to receive sentences from splitter
    pull_socket = context.socket(zmq.PULL)
    splitter_address = f"tcp://{const_wordcount.SPLITTER_HOST}:{const_wordcount.SPLITTER_PORT}"
    pull_socket.connect(splitter_address)
    
    # Create PUSH sockets to send words to reducers
    push_sockets = []
    reducer_addresses = [
        f"tcp://{const_wordcount.REDUCER1_HOST}:{const_wordcount.REDUCER1_PORT}",
        f"tcp://{const_wordcount.REDUCER2_HOST}:{const_wordcount.REDUCER2_PORT}"
    ]
    
    for address in reducer_addresses:
        push_socket = context.socket(zmq.PUSH)
        push_socket.connect(address)
        push_sockets.append(push_socket)
    
    print(f"Mapper {mapper_id} started and connected to splitter and reducers")
    time.sleep(1)  # Give time for connections to establish
    
    while True:
        # Receive sentence from splitter
        message = pickle.loads(pull_socket.recv())
        
        # Check for termination signal
        if message == "TERMINATE":
            print(f"Mapper {mapper_id} received termination signal")
            break
        
        print(f"Mapper {mapper_id} received: {message}")
        
        # Clean and split sentence into words
        # Remove punctuation and convert to lowercase
        clean_sentence = re.sub(r'[^\w\s]', '', message.lower())
        words = clean_sentence.split()
        
        # Send each word to appropriate reducer
        for word in words:
            if word:  # Skip empty strings
                reducer_id = word_to_reducer(word)
                print(f"Mapper {mapper_id} sending '{word}' to reducer {reducer_id}")
                # Send word along with mapper_id for tracking
                push_sockets[reducer_id].send(pickle.dumps((word, mapper_id)))
    
    # Send termination signals to reducers
    print(f"Mapper {mapper_id} sending termination signals to reducers")
    for i, push_socket in enumerate(push_sockets):
        push_socket.send(pickle.dumps(("TERMINATE", mapper_id)))
    
    # Close sockets
    pull_socket.close()
    for socket in push_sockets:
        socket.close()
    context.term()
    
    print(f"Mapper {mapper_id} finished")

if __name__ == "__main__":
    main()