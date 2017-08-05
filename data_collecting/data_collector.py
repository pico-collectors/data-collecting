from data_collecting.protocol import Protocol


class DataCollector:
    """
    The data collector provides the base mechanism to maintain a connection with some producer and receiving data
    streams from it. It calls the defined protocol every time a new connection is established with the producer or a
    new data stream is received. To determine when a data stream is complete it uses the end marker defined by the
    protocol.

    It works in its own thread.
    """

    def __init__(self, protocol: Protocol):
        """ Associates the data collector with a protocol """
        self._protocol = protocol

    def collect_forever(self, producer_address):
        """
        Starts the collecting service for the given producer address.
        The service is run in its own thread. However, this call blocks until the thread finishes. The service can be
        terminated correctly using the 'stop' method.
        """
        pass

    def stop(self):
        """
        Tells the collecting service to stop. The service may not stop right away but it will be stopped
        eventually.
        """
        pass
