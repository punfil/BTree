from contants import Constants
from sys import maxsize

class IndexFileHandler:
    def __init__(self, page_size=4):  # Page size in number of records (which metadata are here)
        self._filename = Constants.INDEXES_FILENAME
        self._loaded_page: IndexFilePage = IndexFilePage(0)
        self._number_of_pages = 1
        self._page_size = page_size * (Constants.INTEGER_SIZE + Constants.FLOAT_SIZE)

    def load_page(self, page_number):
        assert (page_number < self._number_of_pages)
        self._loaded_page = IndexFilePage(page_number)
        with open(self._filename, "rb") as file:
            file.seek(self._page_size * page_number)
            bytes_read = 0
            while bytes_read < self._page_size:
                # Read index
                index = int.from_bytes(file.read(), Constants.LITERAL)
                bytes_read += Constants.INTEGER_SIZE
                if index == maxsize:
                    break # We got to the end
                # Read page number where the record is
                page_number = int.from_bytes(file.read(), Constants.LITERAL)
                bytes_read += Constants.INTEGER_SIZE
                self._loaded_page.add_last_entry(IndexFilePageEntry(index, page_number))

    def save_page(self):
        with open(self._filename, "ab+") as file:
            file.seek(self._page_size * self._loaded_page.page_number)
            bytes_written = 0
            entry = self._loaded_page.remove_first_entry()
            while bytes_written < self._page_size and entry is not None:
                file.write(entry.index.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                bytes_written += Constants.INTEGER_SIZE
                file.write(entry.page_number.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                bytes_written += Constants.INTEGER_SIZE
                entry = self._loaded_page.remove_first_entry()
            while bytes_written < self._page_size:
                file.write(maxsize.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                bytes_written += Constants.INTEGER_SIZE

    def get_records_page_number(self, index):
        page_number = self._loaded_page.get_records_page_number(index)
        if page_number is None:
            # Check if there are more pages to check
            if self._number_of_pages > 1:
                # Determine if the record is on previous page or next one
                if self._loaded_page.get_last_record_index() > index:
                    # This should be on the previous page
                    page_to_load_number = self._loaded_page.page_number - 1
                else:
                    page_to_load_number = self._loaded_page.page_number + 1
                try:
                    assert (0 <= page_to_load_number < self._number_of_pages)
                except AssertionError:
                    return None  # Such record doesn't exist
                self.save_page()
                self.load_page(page_to_load_number)
                return self.get_records_page_number(index)
        return page_number


class IndexFilePage:
    def __init__(self, page_number):
        self._entries = list()
        self._page_number = page_number

    def add_last_entry(self, entry):
        self._entries.append(entry)

    def add_entry_between(self, entry):
        # If it doesn't find anything return len(list) -1
        # If first element is larger return 0
        list_index = max(0, next((index for index, list_entry in enumerate(self._entries) if entry < list_entry),
                                 len(self._entries)) - 1)
        self._entries.insert(list_index, entry)

    def remove_first_entry(self):
        assert (len(self._entries))
        return self._entries.pop(0)

    def remove_entry_between(self, index):
        entry = next((list_entry for list_entry in self._entries if list_entry.index == index), None)
        assert (entry is not None)
        self._entries.remove(entry)

    def get_records_page_number(self, index):
        return next((entry for entry in self._entries if entry.index == index), None)

    def get_first_record_index(self):
        return self._entries[0].index

    def get_last_record_index(self):
        return self._entries[-1].index

    def get_number_of_entries(self):
        return len(self._entries)

    @property
    def page_number(self):
        return self._page_number


class IndexFilePageEntry:
    def __init__(self, index, page_number):
        self._index = index
        self._page_number = page_number

    @property
    def index(self):
        return self._index

    @property
    def page_number(self):
        return self._page_number
