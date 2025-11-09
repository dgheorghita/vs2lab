import rpc
import logging
import time

from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)

# flag to track if callback was executed
callback_executed = False

# callback function to handle the result
def result_callback(result):
    global callback_executed
    print(f"Client (callback function): Received result from server: {result.value}")
    callback_executed = True

cl = rpc.Client()
print("Client: Starting client...")
cl.run()

base_list = rpc.DBList({'foo'})

print("Client: Making async RPC call to append 'bar' to the list...")
acknowledged = cl.append_async('bar', base_list, result_callback)

if acknowledged:
    print("Client: RPC call acknowledged, server is processing...")

    print(f"Client: Doing other work while waiting...")
    for i in range(15):
        if callback_executed:
            break
        print(f"Client: Work")
        time.sleep(1)
    print(f"Client: All done. Exiting.")
else:
    print("Client: Failed to get ACK from server")

cl.stop()
