import rpc
import logging
import time

from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)
logger = logging.getLogger('vs2lab.lab2.rpc.runcl')

cl = rpc.Client()
cl.run()

base_list = rpc.DBList({'foo'})

def handle_result(result_list):
    logger.info("Callback: Result received: {}".format(result_list.value))

req_id = cl.append('bar', base_list, handle_result)
logger.info(f"Sent async request {req_id}")


for i in range(15):
    logger.info(f"Client working on other tasks... {i}")
    time.sleep(1)

cl.stop()
