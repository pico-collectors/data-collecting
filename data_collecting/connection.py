
class Connection:
    """ Adapter for a socket connection that provides simple methods to send and receive data """

    def __init__(self, socket_connection, address):
        """
        Initializes a new connection adapter for the specified socket connection. This initializer should not be
        called directly. Instead, the 'connect' function should be used to create connection objects.

        :param socket_connection: the socket connection to be adapted
        :param address: the address of the 'server' the connection was established with
        """
        self._socket_connection = socket_connection
        self._destination_address = address

    @property
    def destination_address(self):
        """ Returns the address of the server with which the connection was established """
        return self._destination_address

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def receive(self, end='\r\n') -> bytes:
        """ Blocks to receive data from the connection until it finds the 'end' string """
        pass

    def send(self, message: bytes):
        """ Sends a message in bytes through the connection """
        pass

    def close(self):
        """ Closes the connection """
        self._socket_connection.close()


def connect(address) -> Connection:
    """ Establishes a connection with the given address """
    pass
