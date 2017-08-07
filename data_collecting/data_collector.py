import logging
import threading

from data_collecting.connection import connect
from data_collecting.protocol import Protocol, CorruptedDataError

logger = logging.getLogger('data_collector')


class DataCollector(threading.Thread):
    """
    The data collector provides the base mechanism to maintain a connection
    with some producer and receiving data streams from it. It calls the
    defined protocol every time a new connection is established with the
    producer or a new data stream is received. To determine when a data
    stream is complete it uses the end marker defined by the protocol.

    It works in its own thread.
    """

    def __init__(self, producer_address, protocol: Protocol,
                 reconnect_period: float=10.0):
        """ Associates the data collector with a protocol """
        super().__init__()
        self._producer_address = producer_address
        self._protocol = protocol
        self._reconnect_period = reconnect_period
        self._to_stop = threading.Event()

    def run(self):
        """
        Executes the collecting service until the 'stop' method is called
        """
        # This flag indicates if the service was stopped or not
        was_stopped = False

        while not was_stopped:

            logger.info("Trying to connect to %s:%d" %
                        (self._producer_address[0], self._producer_address[1]))

            with connect(self._producer_address) as connection:
                logger.info("Connected successfully")

                # Notify the protocol of the new connection
                self._protocol.on_connection_established(connection)

                while not self._to_stop.is_set():
                    logger.info("Waiting for data...")
                    data = connection.receive(end=self._protocol.end_marker)
                    logger.info("Received a new data item")

                    try:
                        # Notify the protocol of the new data item
                        self._protocol.on_data_received(connection, data)

                    except CorruptedDataError as error:
                        logger.info("%s: %s" % (str(error), data.decode()))

            # Wait some time for to reconnect again or for the service to be
            # stopped
            was_stopped = self._to_stop.wait(timeout=self._reconnect_period)

    def collect_forever(self, producer_address):
        """
        Starts the collecting service for the given producer address. The
        service is run in its own thread. However, this call blocks until the
        thread finishes. The service can be terminated correctly using the
        'stop' method.
        """
        self.start()
        self.join()

    def stop(self):
        """
        Tells the collecting service to stop. The service may not stop right
        away but it will be stopped eventually.
        """
        self._to_stop.set()
