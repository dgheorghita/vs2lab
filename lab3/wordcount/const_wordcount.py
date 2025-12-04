# Configuration for the distributed word counting system

# Splitter configuration (sends sentences to mappers)
SPLITTER_HOST = "127.0.0.1"
SPLITTER_PORT = "5555"

# Reducer configuration (receives words from mappers)
REDUCER1_HOST = "127.0.0.1"
REDUCER1_PORT = "5556"
REDUCER2_HOST = "127.0.0.1"
REDUCER2_PORT = "5557"

# Number of reducers
NUM_REDUCERS = 2