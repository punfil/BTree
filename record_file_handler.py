from contants import Constants
from sys import maxsize
from os import path

class RecordFileHandler:
    def __init__(self, page_size=4):  # Page size in number of records
        self._filename = Constants.RECORDS_FILENAME
        self._loaded_page = RecordFilePage(0)
        self._number_of_pages = 1
        # Index + P(A) + P(B) + P(AUB)
        self._page_size = page_size * (Constants.INTEGER_SIZE + 3 * Constants.FLOAT_SIZE)

    def load_page(self, page_number):
        try:
            assert(path.exists(self._filename))
        except AssertionError:
            return
        assert (0 <= page_number < self._number_of_pages)
        self._loaded_page = RecordFilePage(page_number)
        with open(self._filename, "rb") as file:
            file.seek(self._page_size)


class RecordFilePage:
    def __init__(self, page_number):
        self._records = list()
        self._page_number = page_number

    def load_page(self, page_number):
        pass

    @property
    def page_number(self):
        return self._page_number
