"""
Simple client server unit test
"""

import logging
import threading
import unittest

import clientserver
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)


class TestEchoService(unittest.TestCase):
    """The test"""

    _server = clientserver.Server()  # create single server in class variable
    _server_thread = threading.Thread(
        target=_server.serve
    )  # define thread for running server

    @classmethod
    def setUpClass(cls):
        cls._server_thread.start()  # start server loop in a thread (called only once)

    def setUp(self):
        super().setUp()
        self.client = clientserver.Client()  # create new client for each test
        
    def test_getall(self):
        """Test retrieving the entire phonebook"""
        expected_phonebook = {
            "Alex": "+491514353453",
            "Bob": "+491233423423",
            "Charlie": "+49324234234124234",
        }
        result = self.client.getall()
        self.assertEqual(result, expected_phonebook)

    def test_get_existing_entry(self):
        """Test retrieving a single existing entry"""
        result = self.client.get("Alex")
        self.assertEqual(result, {"Alex": "+491514353453"})
        result = self.client.get("Bob")
        self.assertEqual(result, {"Bob": "+491233423423"})

    def test_get_nonexistent_entry(self):
        """Test retrieving a non-existing entry returns None"""
        result = self.client.get("Alice")
        self.assertEqual(result, {"Alice": None})

    def tearDown(self):
        self.client.close()  # terminate client after each test

    @classmethod
    def tearDownClass(cls):
        cls._server._serving = (
            False  # break out of server loop. pylint: disable=protected-access
        )
        cls._server_thread.join()  # wait for server thread to terminate


if __name__ == "__main__":
    unittest.main()
