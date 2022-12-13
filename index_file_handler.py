from os import path
from sys import maxsize

from contants import Constants


class IndexFileHandler:
    def __init__(self, page_size=4):  # Page size in number of records (which metadata are here)
        self._filename = Constants.INDEXES_FILENAME
        self._loaded_page: IndexFilePage = IndexFilePage(page_size, 0)
        self._loaded_page_stack = []
        self._number_of_pages = 1
        self._page_size = page_size * (3 * Constants.INTEGER_SIZE) + Constants.INTEGER_SIZE
        self._page_size_in_records = page_size

    def load_page(self, page_number):
        try:
            assert (path.exists(self._filename))
            assert (0 <= page_number < self._number_of_pages)
        except AssertionError:
            return
        self._loaded_page = IndexFilePage(self._page_size_in_records, page_number)
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
                            break
                        self._loaded_page.add_last_pointer_entry(IndexFilePageAddressEntry(file_position))
                    case 1:  # It's the index
                        index = int.from_bytes(file.read(Constants.INTEGER_SIZE), Constants.LITERAL)
                        if index == maxsize:
                            break
                    case 2:  # It's the page number
                        page_number_from_file = int.from_bytes(file.read(Constants.INTEGER_SIZE), Constants.LITERAL)
                        self._loaded_page.add_last_metadata_entry(
                            IndexFilePageRecordEntry(index, page_number_from_file))
                bytes_read += Constants.INTEGER_SIZE
                numbers_read += 1
        self._loaded_page.fill()

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

    def pop_last_page_stack(self):
        self._loaded_page = self._loaded_page_stack.pop(0)

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

    def get_number_of_sons(self):
        return self._loaded_page.get_number_of_pointer_entries()

    def add_record(self, index, page_number):
        # When calling that make sure that there's place on this page!
        self._loaded_page.add_metadata_entry_between(IndexFilePageRecordEntry(index, page_number))

    def add_new_page(self):
        self._loaded_page.create_new_page(self._number_of_pages)
        self._number_of_pages += 1

    @property
    def loaded_page(self):
        return self._loaded_page

    @loaded_page.setter
    def loaded_page(self, value):
        self._loaded_page = value


class IndexFilePage:
    def __init__(self, page_size, page_number):
        self._metadata_entries = [None for _ in range(page_size)]
        self._pointer_entries = [None for _ in range(page_size+1)]
        self._page_size = page_size
        self._page_number = page_number

    def fill(self):
        while len(self._metadata_entries) < self._page_size:
            self._metadata_entries.append(None)
        while len(self._pointer_entries) < self._page_size + 1:
            self._pointer_entries.append(None)

    def create_new_page(self, new_page_number):
        self._page_number = new_page_number
        self._pointer_entries = [None for _ in range(24)]
        self._metadata_entries = [None for _ in range(24)]

    def add_last_metadata_entry(self, entry):
        self._metadata_entries[max(self._metadata_entries.index(None), 0)] = entry

    def add_last_pointer_entry(self, entry):
        self._pointer_entries[max(self._pointer_entries.index(None), 0)] = entry

    def add_metadata_entry_between(self, entry):
        new_metadata = []
        new_value_written = False
        for element in self._metadata_entries:
            if element is None and new_value_written is False:
                new_metadata.append(entry)
                new_value_written = True
                break
            if element is None:
                break
            elif element >= entry and new_value_written is False:
                new_metadata.append(entry)
                new_value_written = True
                new_metadata.append(element)
            else:
                new_metadata.append(element)
        self._metadata_entries = new_metadata
        self.fill()

    def add_pointer_entry_between(self, entry, ptr):
        new_pointers = []
        new_value_written = False
        for list_entry, pointer in zip(self._metadata_entries, self._pointer_entries):
            if list_entry is None:
                new_pointers.append(ptr)
                break
            elif entry >= list_entry and new_value_written is False:
                new_pointers.append(ptr)
                new_value_written = True
            else:
                new_pointers.append(pointer)
        self._pointer_entries = new_pointers
        self.fill()

    def remove_metadata_entry_between(self, index):
        entry = next((list_entry for list_entry in self._metadata_entries if list_entry is not None and list_entry.index == index), None)
        try:
            assert (entry is not None)
        except AssertionError:
            return
        self._metadata_entries.remove(entry)

    def remove_pointer_entry_between(self, index):
        list_index = max(0,
                         next(
                             (idx for idx, list_entry in enumerate(self._metadata_entries) if list_entry is not None and index < list_entry.index),
                             len(self._metadata_entries)) - 2)
        self._pointer_entries.pop(list_index)

    def get_records_page_number(self, index):
        return next((entry.page_number for entry in self._metadata_entries if entry is not None and entry.index == index), None)

    def get_number_of_metadata_entries(self):
        return self._page_size - self._metadata_entries.count(None)

    def get_number_of_pointer_entries(self):
        return self._page_size - self._pointer_entries.count(None)

    @property
    def page_number(self):
        return self._page_number

    @property
    def metadata_entries(self):
        return self._metadata_entries

    @property
    def pointer_entries(self):
        return self._pointer_entries


class IndexFilePageAddressEntry:

    def __init__(self, file_position):
        self._file_position = file_position
        super().__init__()

    @property
    def file_position(self):
        return self._file_position


class IndexFilePageRecordEntry:
    def __init__(self, index, page_number):
        self._index = index
        self._page_number = page_number
        super().__init__()

    @property
    def index(self) -> int:
        return self._index

    @index.setter
    def index(self, new_index):
        self._index = new_index

    @property
    def page_number(self) -> int:
        return self._page_number

    def __lt__(self, other):
        return self._index < other.index

    def __le__(self, other):
        return self._index <= other.index

    def __gt__(self, other):
        return self._index > other.index

    def __ge__(self, other):
        return self._index >= other.index
