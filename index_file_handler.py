from abc import ABC
from os import path
from sys import maxsize

from contants import Constants


class IndexFileHandler:
    def __init__(self, page_size=4):  # Page size in number of records (which metadata are here)
        self._filename = Constants.INDEXES_FILENAME
        self._loaded_page: IndexFilePage = IndexFilePage(0)
        self._loaded_page_stack = []
        self._number_of_pages = 1
        self._page_size = page_size * (3 * Constants.INTEGER_SIZE) + Constants.INTEGER_SIZE

    def load_page(self, page_number):
        try:
            assert (path.exists(self._filename))
            assert (0 <= page_number < self._number_of_pages)
        except AssertionError:
            return
        self._loaded_page = IndexFilePage(page_number)
        with open(self._filename, "rb") as file:
            file.seek(self._page_size * page_number)
            bytes_read = 0
            numbers_read = 0
            index, page_number_from_file = 0, 0
            while bytes_read < self._page_size:
                match numbers_read % 3:
                    case 0:  # It's the address
                        file_position = int.from_bytes(file.read(Constants.INTEGER_SIZE), Constants.LITERAL)
                        if file_position == maxsize:
                            return
                        self._loaded_page.add_last_pointer_entry(IndexFilePageAddressEntry(file_position))
                    case 1:  # It's the index
                        index = int.from_bytes(file.read(Constants.INTEGER_SIZE), Constants.LITERAL)
                        if index == maxsize:
                            return
                    case 2:  # It's the page number
                        page_number_from_file = int.from_bytes(file.read(Constants.INTEGER_SIZE), Constants.LITERAL)
                        self._loaded_page.add_last_metadata_entry(
                            IndexFilePageRecordEntry(index, page_number_from_file))
                bytes_read += Constants.INTEGER_SIZE
                numbers_read += 1

    def put_current_page_on_page_stack(self):
        self._loaded_page_stack.append(self._loaded_page)

    def load_page_from_page_stack(self, index):
        try:
            assert (0 <= index < len(self._loaded_page_stack))
        except AssertionError:
            return
        self._loaded_page = self._loaded_page_stack[index]

    def empty_page_stack(self):
        self._loaded_page_stack.clear()

    def save_page(self):
        with open(self._filename, "ab+") as file:
            file.seek(self._page_size * self._loaded_page.page_number)
            bytes_written = 0
            entry = self._loaded_page.remove_first_metadata_entry()
            while bytes_written < self._page_size and entry is not None:
                file.write(entry.index.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                bytes_written += Constants.INTEGER_SIZE
                file.write(entry.page_number.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                bytes_written += Constants.INTEGER_SIZE
                entry = self._loaded_page.remove_first_metadata_entry()
            while bytes_written < self._page_size:
                file.write(maxsize.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                bytes_written += Constants.INTEGER_SIZE

    def get_records_page_number(self, index):
        page_number = self._loaded_page.get_records_page_number(index)
        return page_number

    def get_number_of_records(self):
        return self._loaded_page.get_number_of_metadata_entries()

    @property
    def loaded_page(self):
        return self._loaded_page


class IndexFilePage:
    def __init__(self, page_number):
        self._metadata_entries = [IndexFilePageRecordEntry]
        self._pointer_entries = [IndexFilePageAddressEntry]
        self._page_number = page_number

    def add_last_metadata_entry(self, entry):
        self._metadata_entries.append(entry)

    def add_last_pointer_entry(self, entry):
        self._pointer_entries.append(entry)

    def add_metadata_entry_between(self, entry):
        # If it doesn't find anything return len(list) -1
        # If first element is larger return 0
        list_index = max(0,
                         next((index for index, list_entry in enumerate(self._metadata_entries) if entry < list_entry),
                              len(self._metadata_entries)) - 1)
        self._metadata_entries.insert(list_index, entry)

    def add_pointer_entry_between(self, entry, ptr_value):
        # If it doesn't find anything return len(list) -1
        # If first element is larger return 0
        list_index = max(0,
                         next((index for index, list_entry in enumerate(self._metadata_entries) if entry < list_entry),
                              len(self._metadata_entries)) - 2)
        self._pointer_entries.insert(list_index, ptr_value)

    def remove_first_metadata_entry(self):
        assert (len(self._metadata_entries))
        return self._metadata_entries.pop(0)

    def remove_first_pointer_entry(self):
        assert (len(self._pointer_entries))
        return self._pointer_entries.pop(0)

    def remove_metadata_entry_between(self, index):
        entry = next((list_entry for list_entry in self._metadata_entries if list_entry.index == index), None)
        try:
            assert (entry is not None)
        except AssertionError:
            return
        self._metadata_entries.remove(entry)

    def remove_pointer_entry_between(self, index):
        list_index = max(0,
                         next(
                             (idx for idx, list_entry in enumerate(self._metadata_entries) if index < list_entry.index),
                             len(self._metadata_entries)) - 2)
        self._pointer_entries.pop(list_index)

    def get_records_page_number(self, index):
        return next((entry.page_number for entry in self._metadata_entries if entry.index == index), None)

    def get_first_record_index(self):
        return self._metadata_entries[0].index

    def get_last_record_index(self):
        return self._metadata_entries[-1].index

    def get_number_of_metadata_entries(self):
        return len(self._metadata_entries)

    def get_particular_metadata_entry(self, index):
        try:
            assert (index < len(self._metadata_entries))
        except AssertionError:
            return None
        return self._metadata_entries[index]

    def get_particular_pointer_entry(self, index):
        try:
            assert (index < len(self._pointer_entries))
        except AssertionError:
            return None
        return self._pointer_entries[index]

    @property
    def page_number(self):
        return self._page_number


class IndexFilePageEntry(ABC):
    def __init__(self):
        pass


class IndexFilePageAddressEntry(IndexFilePageEntry):
    def __init__(self, file_position):
        self._file_position = file_position
        super().__init__()

    @property
    def file_position(self):
        return self._file_position


class IndexFilePageRecordEntry(IndexFilePageEntry):
    def __init__(self, index, page_number):
        self._index = index
        self._page_number = page_number
        super().__init__()

    @property
    def index(self) -> int:
        return self._index

    @property
    def page_number(self) -> int:
        return self._page_number
