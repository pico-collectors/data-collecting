from data_collecting.connection import Connection


class CorruptedDataError(Exception):
    """
    Raised by the Protocol when the data received is corrupted or the
    format is not correct.
    """
    pass


class Protocol:
    """
    The base class to describe the protocol to communicate with the data
    producers.
    """

    @property
    def end_marker(self) -> bytes:
        """
        Returns the marker that marks the end of each data stream expected by
        this protocol. Subclasses should override this method to adjust for
        they supported marker. If the subclass does not override this
        property getter it assumes the marker is '\r\n' by default.
        """
        return b'\r\n'

    def on_connection_established(self, connection: Connection):
        """ Invoked by the DataCollector once a connection is established """
        pass

    def on_data_received(self, connection: Connection, data: bytes):
        """
        Invoked by the DataCollector once a new stream of data is received.
        The data parameter contains the data stream that was obtained by the
        data collector. It does NOT contain the end marker.

        The protocol should decode the data stream and convert it into a data
        item and call the respective data handlers with the decoded item.
        """
        pass
