import rpc
import logging
import time

from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)

def on_result(result):
    print("Result received:", result.value)

cl = rpc.Client()
cl.run()

base_list = rpc.DBList({'foo'})
cl.append('test', base_list, on_result)
for i in range(15):
  print("client is working something else")
  time.sleep(1)



cl.stop()
