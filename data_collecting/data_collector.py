import logging
import socket
import threading

from data_collecting.connection import connect, Connection
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
                 reconnect_period=10.0, message_period=60.0):
        """
        Associates the data collector with a protocol

        :param producer_address: address of the producer to collect data from
        :param protocol:         protocol used to communicate with the producer
        :param reconnect_period: period between attempts to reconnect
        :param message_period:   period with which new messages are expected
        """
        super().__init__()
        self._producer_address = producer_address
        self._protocol = protocol
        self._reconnect_period = reconnect_period
        self._message_period = message_period

        # Event that indicates the service should stop
        self._to_stop = threading.Event()

    def run(self):
        """
        Executes the collecting service until the 'stop' method is called
        """
        # This flag indicates if the service was stopped or not
        was_stopped = False

        while not was_stopped:

            try:
                with self._connect() as connection:
                    while not self._to_stop.is_set():
                        self._collect(connection)

            except (socket.herror, socket.gaierror):
                # Unrecoverable error, must stop the service!
                logger.error("The address of the producer is not valid")
                # Exit immediately
                break

            # The program reaches this point when the connection with the
            # producer fails
            logger.warning("It will try to reconnect in approximately %d "
                           "seconds" % int(self._reconnect_period))

            # Wait some time before reconnecting again
            # Meanwhile, if the stop() method is called then it stops waiting
            # and exits the service
            was_stopped = self._to_stop.wait(timeout=self._reconnect_period)

    def collect_forever(self):
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

    def _connect(self):
        """
        This works as a context manager. It connects to the producer and
        yields the established connection.

        It also handles connection errors. If the connection fails inside
        the context then it logs a warning with the type of error.

        It may raise a socket.herror or a socket.gaierror if the address of
        the producer is not valid.
        """

        try:
            logger.info("Trying to connect to %s:%d" % self._producer_address)
            with connect(self._producer_address,
                         self._message_period) as connection:
                logger.info("Connected successfully")

                # Notify the protocol of the new connection
                self._protocol.on_connection_established(connection)
                yield connection

        except ConnectionAbortedError:
            logger.warning("Connection aborted by the producer")
        except socket.timeout:
            logger.warning("Connection timed out")
        except ConnectionRefusedError:
            logger.warning("Cannot reach the producer")
        except OSError as error:
            logger.warning("OSError: %s" % str(error))

    def _collect(self, connection: Connection):
        """
        Blocks waiting for new data. It notifies the protocol when a new
        complete data stream is received.
        """
        logger.info("Waiting for data...")
        data = connection.receive(end=self._protocol.end_marker)
        logger.info("Received a new data item")

        try:
            # Notify the protocol of the new data item
            self._protocol.on_data_received(connection, data)

        except CorruptedDataError as error:
            logger.info("%s: %s" % (str(error), data.decode()))
