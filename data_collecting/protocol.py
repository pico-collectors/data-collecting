from data_collecting.connection import Connection


class Protocol:
    """ The base class to describe the protocol to communicate with the data producer """

    @property
    def end_marker(self):
        """
        Returns the marker that marks the end of each data stream expected by this protocol. Subclasses should
        override this method to adjust for they supported marker. If the subclass does not override this property
        getter it assumes the marker is '\r\n' by default.
        """
        return '\r\n'

    def on_connection_established(self, connection: Connection):
        """ Invoked by the DataCollector once a connection is established """
        pass

    def on_data_received(self, connection: Connection, data: bytes):
        """
        Invoked by the DataCollector once a new stream of data is received. The data parameter contains the data
        stream that was obtained by the data collector. It does NOT contain the end marker.
        """
        pass