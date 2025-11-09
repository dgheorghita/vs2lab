import constRPC
import time
import threading

from context import lab_channel


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

        self.response_thread = None
        self.running = False
        self.ack_received = threading.Event()

    def run(self):
        self.chan.bind(self.client)
        self.server = self.chan.subgroup('server')

        self.running = True
        # start response handler thread
        self.response_thread = threading.Thread(target=self._response_handler)
        self.response_thread.daemon = True
        print("Client: Starting response handler thread in the background...")
        self.response_thread.start()

    def stop(self):
        print("Client: Stopping client and response handler thread...")
        self.running = False
        if self.response_thread:
            self.response_thread.join(timeout=1)
        self.chan.leave('client')

    def _response_handler(self): # runs in the background
        while self.running:
            try:
                msgrcv = self.chan.receive_from_any(timeout=1)
                if msgrcv is not None:
                    sender = msgrcv[0]
                    msg_data = msgrcv[1]
                    
                    if isinstance(msg_data, tuple) and len(msg_data) > 0: # expecting a non-empty tuple
                        msg_type = msg_data[0]
                        if msg_type == constRPC.ACK: # if ACK message
                            print("Client (background thread): Received ACK from server")
                            self.ack_received.set()  # signal main thread that ACK was received
                        elif msg_type == constRPC.OK and len(msg_data) > 1: # if OK message (final result)
                            result = msg_data[1]
                            print("Client (background thread): Received a response from server")
                            if hasattr(self, 'current_callback') and self.current_callback: # if callback is set
                                print("Client (background thread): Calling the callback function with the result")
                                self.current_callback(result)
                            else:
                                print("Client (background thread): Result received, but no callback function set to handle the result")
                        else:
                            print(f"Client (background thread): Unknown message type: {msg_type}")
                    else:
                        print(f"Client (background thread): Invalid message format received: {type(msg_data)} - {msg_data}")
            except Exception as e:
                if self.running:  # Only print if we're supposed to be running
                    print(f"Client (background thread): Client response handler error: {e}")

    def append_async(self, data, db_list, callback=None):
        assert isinstance(db_list, DBList)
        self.current_callback = callback
        
        # reset ACK synchronization
        self.ack_received.clear()
        
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send msg to server
        
        # wait for ACK
        if self.ack_received.wait(timeout=5):
            print("Client: Received ACK from server")
            return True  # unblocks the main thread, so client can do other work
        else:
            print("Client: No response received")
            return False

    def append(self, data, db_list):
        assert isinstance(db_list, DBList)

        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send msg to server

        msgrcv = self.chan.receive_from(self.server)  # wait for response
        return msgrcv[1]  # pass it to caller


class Server:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.server = self.chan.join('server')
        self.timeout = 3

    @staticmethod
    def append(data, db_list):
        assert isinstance(db_list, DBList)  # - Make sure we have a list
        return db_list.append(data)

    def process_request_async(self, client, data, db_list):
        print("Server (background thread): Processing APPEND request. ETA 10 seconds...")
        time.sleep(10)  # pause to simulate long processing

        print("Server (background thread): Finished processing APPEND request")
        result = self.append(data, db_list)
        self.chan.send_to({client}, (constRPC.OK, result))
        print("Server (background thread): Sent response to client. Waiting for next request...")

    def run(self):
        self.chan.bind(self.server)
        print("Server: Running and waiting for requests...")
        while True:
            msgreq = self.chan.receive_from_any(self.timeout)  # wait for any request
            if msgreq is not None:
                client = msgreq[0]  # see who is the caller
                msgrpc = msgreq[1]  # fetch call & parameters

                if constRPC.APPEND == msgrpc[0]:  # capture APPEND requests
                    print("Server: Received APPEND request from client")

                    print("Server: Sending immediate ACK to client")
                    self.chan.send_to({client}, (constRPC.ACK,))
                    
                    # process request in separate thread
                    ## configure thread
                    thread = threading.Thread(
                        target=self.process_request_async,
                        args=(client, msgrpc[1], msgrpc[2])
                    )
                    thread.daemon = True # so that SIGTERM is respected

                    ## start thread
                    print("Server: Starting async request-processing thread in the background and waiting for next request")
                    thread.start()

                else:
                    print("Server: Ignoring unsupported request")
                    pass  # unsupported request, simply ignore
