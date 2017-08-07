import socket


class Connection:
    """
    Adapter for a socket connection that provides simple methods to send
    and receive data.
    """

    def __init__(self, socket_connection, address):
        """
        Initializes a new connection adapter for the specified socket
        connection. This initializer should not be called directly. Instead,
        the 'connect' function should be used to create connection objects.

        :param socket_connection: the socket connection to be adapted
        :param address: the address of the 'server' the connection was
                        established with
        """
        self._socket_connection = socket_connection   # type: socket.socket
        self._destination_address = address

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

    def receive(self, timeout=60.0, end=b'\r\n') -> bytes:
        """
        Blocks to receive data from the connection until it finds the
        'end' string.
        """
        # Set the receive timeout
        self._socket_connection.settimeout(timeout)

        # Get the extra bytes received in the previous call
        data = self._recv_cache

        # Clear the cached bytes
        self._recv_cache = bytes()

        while True:
            # The verification of end of data must be the first step of the loop
            # because the receive cache may contain a complete line

            end_index = data.find(end)
            if end_index != -1:
                # The data item is complete

                # Store extra bytes that have been received in the cache
                self._recv_cache = data[end_index + 2:]

                # Remove extra bytes from the data stream that is going to be
                # returned
                data = data[0:end_index]
                break

            buffer = self._socket_connection.recv(512)

            if not buffer:
                raise ConnectionAbortedError("Connection with sender was "
                                             "correctly closed")

            # TODO investigate this condition a little bit
            # The goal here appears to be setting a 1 minute timeout after
            # the first chunk of data is received from the producer
            # My understanding is that it is possible that the producer sends
            # an incomplete data item that does not contain the end marker

            # TODO make the 1 minute timeout configurable

            if not data:  # check if this is the first data chunk
                # after receiving data chunk set a timeout of 1 minute
                # this timeout prevents errors due to the server not finishing
                # the transmission
                self._socket_connection.settimeout(60.0)

            data += buffer

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


def connect(address, timeout=30.0) -> Connection:
    """ Establishes a connection with the given address """
    sock_connection = socket.create_connection(address, timeout)
    return Connection(sock_connection, address)
