class DataHandler:
    """ Base class to implement classes to handle new data items """

    def process(self, data):
        """
        Processes a new data item.

        This method is invoked by collector implementations once they decode
        the data from a data byte stream. The actual decoding structure of
        the data item should be agreed between the collector calling the
        handler and the data handler implementation.

        :param data: the data item to process
        """
        pass
