import pickle
import sys
import time
import zmq
import const_wordcount

def main():
    if len(sys.argv) != 2:
        print("Usage: python reducer.py <reducer_id>")
        sys.exit(1)
    
    reducer_id = int(sys.argv[1])
    
    # Determine which port this reducer should use
    if reducer_id == 0:
        host = const_wordcount.REDUCER1_HOST
        port = const_wordcount.REDUCER1_PORT
    elif reducer_id == 1:
        host = const_wordcount.REDUCER2_HOST
        port = const_wordcount.REDUCER2_PORT
    else:
        print(f"Invalid reducer ID: {reducer_id}. Use 0 or 1.")
        sys.exit(1)
    
    # Create ZeroMQ context and socket
    context = zmq.Context()
    pull_socket = context.socket(zmq.PULL)
    
    # Bind to address where mappers will connect
    address = f"tcp://{host}:{port}"
    pull_socket.bind(address)
    
    print(f"Reducer {reducer_id} started on {address}")
    print(f"Reducer {reducer_id} handles words: {'A-M' if reducer_id == 0 else 'N-Z'}")
    
    # Dictionary to store word counts
    word_counts = {}
    
    # Keep track of how many mappers have terminated
    terminated_mappers = set()
    total_mappers = 3  # We have 3 mappers
    
    while True:
        # Receive word from a mapper
        message = pickle.loads(pull_socket.recv())
        word, mapper_id = message
        
        # Check for termination signal
        if word == "TERMINATE":
            terminated_mappers.add(mapper_id)
            print(f"Reducer {reducer_id} received termination from mapper {mapper_id}")
            
            # If all mappers have terminated, we can stop
            if len(terminated_mappers) == total_mappers:
                print(f"Reducer {reducer_id} received termination from all mappers")
                break
            continue
        
        # Count the word
        if word in word_counts:
            word_counts[word] += 1
        else:
            word_counts[word] = 1
        
        print(f"Reducer {reducer_id} received '{word}' from mapper {mapper_id} -> count: {word_counts[word]}")
    
    # Print final results
    print(f"\nReducer {reducer_id} FINAL RESULTS:")
    print("=" * 40)
    for word in sorted(word_counts.keys()):
        print(f"{word}: {word_counts[word]}")
    print(f"Total unique words handled by reducer {reducer_id}: {len(word_counts)}")
    print(f"Total word instances processed: {sum(word_counts.values())}")
    
    pull_socket.close()
    context.term()
    print(f"Reducer {reducer_id} finished")

if __name__ == "__main__":
    main()