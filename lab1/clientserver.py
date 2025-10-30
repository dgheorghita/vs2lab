"""
Client and server using classes
"""

import logging
import socket
import json
from typing import Dict

import const_cs
from context import lab_logging

lab_logging.setup(stream_level=logging.DEBUG)  # init loging channels for the lab

# pylint: disable=logging-not-lazy, line-too-long


class Server:
    """The server"""

    _logger = logging.getLogger("vs2lab.lab1.clientserver.Server")
    _serving = True

    def __init__(self):
        self.phonebook = {"Alex": "+491514353453", "Bob": "+491233423423", "Charlie": "+49324234234124234"}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
        )  # prevents errors due to "addresses in use"
        self.sock.bind((const_cs.HOST, const_cs.PORT))
        self.sock.settimeout(3)  # time out in order not to block forever
        self._logger.info("Server bound to socket " + str(self.sock))

    def serve(self):
        """Serve echo"""
        self.sock.listen(1)
        while (
            self._serving
        ):  # as long as _serving (checked after connections or socket timeouts)
            try:
                # pylint: disable=unused-variable
                (connection, address) = (
                    self.sock.accept()
                )  # returns new socket and address of client
                while True:  # forever
                    data = connection.recv(1024)  # receive data from client
                    if not data:
                        break  # stop if client stopped
                    request = data.decode("ascii")
                    if request == "*":
                        response = json.dumps(self.phonebook)
                    else:
                        response = json.dumps(
                            {request: self.phonebook.get(request, None)}
                        )
                    connection.send(
                        response.encode("utf-8")
                    )  # return sent data plus an "*"
                connection.close()  # close the connection
            except socket.timeout:
                pass  # ignore timeouts
        self.sock.close()
        self._logger.info("Server down.")


class Client:
    """The client"""

    logger = logging.getLogger("vs2lab.a1_layers.clientserver.Client")

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((const_cs.HOST, const_cs.PORT))
        self.logger.info("Client connected to socket " + str(self.sock))

    def getall(self):
        self.logger.info("Attempting getting all entries")
        self.sock.send("*".encode("ascii"))
        self.logger.debug("Waiting for response...")
        data = self.sock.recv(1024)
        self.logger.debug("Received response")
        return json.loads(data.decode("ascii"))

    def get(self, name: str):
        self.logger.info(f"Attempting getting entry for {name}")
        self.sock.send(name.encode("ascii"))
        self.logger.debug("Waiting for response...")
        data = self.sock.recv(1024)
        self.logger.debug("Received response")
        return json.loads(data.decode("ascii"))

    def close(self):
        """Close socket"""
        self.logger.debug("Closing socket")
        self.sock.close()
