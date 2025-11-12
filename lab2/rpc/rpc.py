import constRPC
import threading
import uuid
import time
from context import lab_channel
import logging
logger = logging.getLogger('vs2lab.lab2.rpc.server')


class DBList:
    def __init__(self, basic_list):
        self.value = list(basic_list)

    def append(self, data):
        self.value = self.value + [data]
        return self


class Client:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.client = self.chan.join('client')
        self.server = None
        self.callbacks = {}
        self.running = False

    def run(self):
        self.chan.bind(self.client)
        self.server = self.chan.subgroup('server')
        self.running = True
        threading.Thread(target=self._listen_thread, daemon=True).start() # starts a listening Thread

    def stop(self):
        self.running = False
        self.chan.leave('client')
        
    def _listen_thread(self):
        while self.running:
            msg = self.chan.receive_from_any(timeout=1)
            if msg is None:
                continue

            msg_type = msg[0]
            req_id = msg[1]

            if msg_type == constRPC.ACK:
                logger.info(f"[Client] ACK received for request {req_id}")
            elif msg_type == constRPC.RESULT:
                result = msg[2]
                logger.info(f"[Client] Result received for request {req_id}")
                if req_id in self.callbacks:
                    cb = self.callbacks.pop(req_id)
                    cb(result)
            else:
                logger.info(f"[Client] Unknown message type: {msg}")

    def append(self, data, db_list, callback):
        assert isinstance(db_list, DBList)
        req_id = str(uuid.uuid4()) # creates request ID
        msg = (constRPC.APPEND, req_id, data, db_list) # message payload
        self.callbacks[req_id] = callback # register callback
        self.chan.send_to(self.server, msg) # send msg to server
        logger.info(f"[Client] Sent async request {req_id}")
        return req_id


class Server:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.server = self.chan.join('server')
        self.timeout = 3

    @staticmethod
    def append(data, db_list):
        assert isinstance(db_list, DBList)  # - Make sure we have a list
        return db_list.append(data)

    def run(self):
        self.chan.bind(self.server)
        logger.info("[Server] Running...")
        while True:
            msgreq = self.chan.receive_from_any(self.timeout)
            if msgreq is None:
                continue

            client = msgreq[0]
            msgrpc = msgreq[1]
            msg_type = msgrpc[0]

            if msg_type == constRPC.APPEND:
                req_id = msgrpc[1]
                data = msgrpc[2]
                db_list = msgrpc[3]

                # send ACK 
                self.chan.send_to({client}, (constRPC.ACK, req_id))
                logger.info(f"[Server] ACK sent for {req_id}")

                threading.Thread(
                    target=self._handle_request,
                    args=(client, req_id, data, db_list),
                    daemon=True
                ).start()

    def _handle_request(self, client, req_id, data, db_list):
        logger.info(f"[Server] Started processing request {req_id}, simulating task...")
        time.sleep(10)
        result = self.append(data, db_list)
        self.chan.send_to({client}, (constRPC.RESULT, req_id, result))
        logger.info(f"[Server] Result sent for {req_id}")
