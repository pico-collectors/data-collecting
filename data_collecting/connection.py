import socket
import time
from contextlib import contextmanager


class Connection:
    """
    Adapter for a socket connection that provides simple methods to send
    and receive data.
    """

    def __init__(self, socket_connection, address, recv_timeout: float,
                 max_msg_delay: float):
        """
        Initializes a new connection adapter for the specified socket
        connection. This initializer should not be called directly. Instead,
        the 'connect' function should be used to create connection objects.

        The parameters recv_timeout and max_msg_delay can be confusing. They
        have very different purposes. The recv_timeout dictates the amount of
        the time the receive/send method will block until a one or more bytes
        of a message are received/sent. This is very different from
        max_msg_delay which indicates the maximum amount of time the receive
        method should block until a new message is complete after it receives
        the first bytes.

        :param socket_connection: the socket connection to be adapted
        :param address:           the address of the 'server' the connection
                                  was established with
        :param recv_timeout:      the timeout for receiving new data
        :param max_msg_delay:     the maximum amount of time that a message
                                  can take to be sent
        """
        self._socket_connection = socket_connection  # type: socket.socket
        self._destination_address = address

        self._recv_timeout = recv_timeout
        self._max_msg_delay = max_msg_delay

        # Stores the data that may have been transferred during a receive call
        # and did not belong to the current data item
        self._recv_cache = bytes()

    @property
    def destination_address(self):
        """
        Returns the address of the server with which the connection was
        established.
        """
        return self._destination_address

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def receive(self, end=b'\r\n') -> bytes:
        """
        Blocks to receive data from the connection until it finds the
        'end' string.
        """
        with self._keep_timeout(self._recv_timeout):

            # Get the extra bytes received in the previous call
            data = self._recv_cache
            # Clear the cached bytes
            self._recv_cache = bytes()

            # start_time stores the time in which the first chunk of data was
            # received. If data was read in the previous call to receive()
            # then it considers the start time to be the time at which
            # receive() was called
            start_time = time.time() if data else None

            while True:
                # The verification of end of data must be the first step of
                # the loop because the receive cache may contain a complete
                # line
                end_index = data.find(end)
                if end_index != -1:
                    # The data item is complete

                    # Store extra bytes that have been received in the cache
                    self._recv_cache = data[end_index + 2:]

                    # Remove extra bytes from the data stream that is going
                    # to be returned
                    data = data[0:end_index]
                    break

                # Block waiting for a new chunk of data
                # It may raise a timeout error if no data is received before
                # the currently specified timeout
                buffer = self._socket_connection.recv(512)

                if not buffer:
                    raise ConnectionAbortedError(
                        "Connection with sender was correctly closed")

                # Add the new bytes to the data buffer
                data += buffer

                # Update the receive timeout according to the _max_msg_delay
                if not start_time:
                    self._set_timeout(self._max_msg_delay)
                    start_time = time.time()
                else:
                    time_elapsed = time.time() - start_time
                    self._set_timeout(self._max_msg_delay - time_elapsed)

        return data

    def send(self, message: bytes, end=b''):
        """ Sends a message in bytes through the connection """
        self._socket_connection.sendall(message + end)

    def close(self):
        """
        Closes the connection. The connection can not be used after a call
        to this method.
        """
        self._socket_connection.close()

    @contextmanager
    def _keep_timeout(self, timeout):
        self._set_timeout(timeout)
        try:
            yield
        finally:
            self._set_timeout(timeout)

    def _set_timeout(self, timeout):
        self._socket_connection.settimeout(timeout)


def connect(address, connect_timeout=30.0, recv_timeout=60.0,
            max_msg_delay=10.0) -> Connection:
    """ Establishes a connection with the given address """
    sock_connection = socket.create_connection(address, connect_timeout)
    return Connection(sock_connection, address, recv_timeout, max_msg_delay)
