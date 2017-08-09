import logging
import socket
import threading
from contextlib import contextmanager

from data_collecting.connection import connect, Connection

logger = logging.getLogger('data_collector')


class CorruptedDataError(Exception):
    """
    Raised by the collector when the data received is corrupted or the
    format is not correct.
    """
    pass


class BaseDataCollector(threading.Thread):
    """
    The base data collector provides the basis to implement any data
    collector class. It works in its own thread.

    It implements the base mechanism to maintain a connection with some
    producer and to receive data streams from it. It defines two abstract
    methods that subclasses must implement in order to provide any useful
    operation to the data collector.
        - on_connection_established:    this method is invoked every time
            the data collector establishes a new connection with the producer.
            This may occur multiple times during an execution, because
            connections can fail and may need to be re-established.

        - on_data_received:             this method is invoked every time a
            new data stream is received from the producer. The data stream
            received is passed as an argument. Subclasses should use this
            method to implement the operations that need to be performed with
            the data.

    The base data collector also defines a read-only property 'end_maker',
    which the value may be overridden when necessary. This end marker is used
    to determine the end of a data stream. By default, the value of this
    property is '\r\n'.

    Note: the base data collector only handles bytes. This means that
    subclasses are supposed to convert any data stream in bytes to the
    required format.
    """

    def __init__(self, producer_address, reconnect_period=10.0,
                 message_period=60.0, max_msg_delay=10.0):
        """
        Defines the address of the producer from which the data will be
        collected. It also defines some settings for the connections.

        :param producer_address: address of the producer to collect data from
        :param reconnect_period: period between attempts to reconnect
        :param message_period:   period with which new messages are expected
        """
        super().__init__()
        self._producer_address = producer_address
        self._reconnect_period = reconnect_period
        self._message_period = message_period
        self._max_msg_delay = max_msg_delay

        # Event that indicates the service should stop
        self._to_stop = threading.Event()

    def run(self):
        """
        Executes the collecting service until the 'stop' method is called.
        It tries to maintain a connection with the producer and receive new
        data stream from it.
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
            except socket.timeout:
                logger.warning("Connection timed out")
            except ConnectionRefusedError:
                logger.warning("Cannot reach the producer")

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

    @contextmanager
    def _connect(self):
        """
        This works as a context manager. It connects to the producer and
        yields the established connection.

        It also handles connection errors. If the connection fails inside
        the context then it logs a warning with the type of error.

        It may raise a socket.herror or a socket.gaierror if the address of
        the producer is not valid.
        """
        logger.info("Trying to connect to %s:%d" % self._producer_address)
        with connect(self._producer_address,
                     recv_timeout=self._message_period,
                     max_msg_delay=self._max_msg_delay) as connection:

            logger.info("Connected successfully")

            # Notify the subclass of the new connection
            self.on_connection_established(connection)

            try:
                yield connection

            except ConnectionAbortedError:
                logger.warning("Connection aborted by the producer")
            except socket.timeout:
                logger.warning("Connection timed out")
            except OSError as error:
                logger.warning("OSError: %s" % str(error))

    def _collect(self, connection: Connection):
        """
        Blocks waiting for a new data stream. Once a new data stream is
        complete it sends it to the subclass and returns afterwards.
        It logs a warning if the data stream is corrupted.
        """
        logger.info("Waiting for data...")
        data = connection.receive(end=self.end_marker)
        logger.info("Received a new data item")

        try:
            # Let the subclass handle the received data
            self.on_data_received(connection, data)

        except CorruptedDataError as error:
            logger.warning("%s: %s" % (str(error), data.decode()))

    @property
    def end_marker(self) -> bytes:
        """
        Returns the marker that marks the end of each data stream expected by
        this collector. Subclasses should override this method to adjust for
        they supported marker. If the subclass does not override this
        property getter, it assumes the marker is '\r\n' by default.
        """
        return b'\r\n'

    def on_connection_established(self, connection: Connection):
        """ Invoked by the collector once a connection is established """
        pass

    def on_data_received(self, connection: Connection, data: bytes):
        """
        Invoked by the DataCollector once a new stream of data is received.
        The data parameter contains the data stream that was obtained by the
        data collector. It does NOT contain the end marker.

        The subclass should decode the data stream and convert it into a data
        item in any format that it requires and call the respective data
        handlers with the decoded item.
        """
        pass
