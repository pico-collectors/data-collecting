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

import sys

from data_collecting.exceptions import UnrecoverableError

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
            logger.info("Stopping the collector...")

        except UnrecoverableError as error:
            logger.error(str(error))
            sys.exit(1)

        except:
            logger.exception("Program was closed due to an unexpected error.")
            sys.exit(1)

        logger.info("Stopped collecting data")


def raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt
