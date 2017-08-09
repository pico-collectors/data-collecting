"""DataTaker

Usage:
  ./datataker <config_file>
  ./datataker (-h | --help)

Options:
  -h --help     Show this screen.
  --version     Show version.

"""
import logging
import signal

logger = logging.getLogger('')


class Runner:
    """ Executes a data collector """

    def __init__(self, collector):
        self._collector = collector

    def run(self):
        """ Runs the program """

        # Ensure the TERMINATE signals are handled in the same way as
        # KeyboardInterrupts
        signal.signal(signal.SIGTERM, raise_keyboard_interrupt)

        try:
            logger.info("Started collecting data")
            self._collector.collect_forever()

        except KeyboardInterrupt:
            # user pressed Ctrl-C to close the program
            self._stop_collecting()

        except:
            logger.exception("Program was closed due to an unexpected error.")
            self._stop_collecting()

        logger.info("Stopped collecting data")

    @staticmethod
    def _stop_collecting():
        logger.info("Stopping the collector...")


def raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt
