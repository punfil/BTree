import sys
from os import path
from sys import maxsize

from contants import Constants


class IndexFileHandler:
    def __init__(self, page_size=4):  # Page size in number of records (which metadata are here)
        self._filename = Constants.INDEXES_FILENAME
        self._loaded_page: IndexFilePage = IndexFilePage(page_size, 0, True)
        self._loaded_page_stack = []
        self._number_of_pages = 1
        self._page_size = page_size * (3 * Constants.INTEGER_SIZE) + Constants.INTEGER_SIZE
        self._page_size_in_records = page_size
        self._number_of_reads = 0
        self._number_of_writes = 0

    def clear_io_operations_counters(self):
        self._number_of_reads = 0
        self._number_of_writes = 0

    def get_io_operations(self):
        return self._number_of_reads, self._number_of_writes

    def load_page(self, page_number):
        try:
            assert (path.exists(self._filename))
            assert (0 <= page_number < self._number_of_pages)
        except AssertionError:
            return
        self._number_of_reads += 1
        self._loaded_page = IndexFilePage(self._page_size_in_records, page_number, True)
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
                            self._loaded_page.add_last_pointer_entry(None)
                        else:
                            self._loaded_page.add_last_pointer_entry(IndexFilePageAddressEntry(file_position))
                            self._loaded_page.is_leaf = False
                    case 1:  # It's the index
                        index = int.from_bytes(file.read(Constants.INTEGER_SIZE), Constants.LITERAL)
                        if index == maxsize:
                            break
                    case 2:  # It's the page number
                        page_number_from_file = int.from_bytes(file.read(Constants.INTEGER_SIZE), Constants.LITERAL)
                        self._loaded_page.add_last_metadata_entry(
                            IndexFilePageRecordEntry(index, page_number_from_file))
                        self._loaded_page.keys_count += 1
                bytes_read += Constants.INTEGER_SIZE
                numbers_read += 1
        self._loaded_page.fill()

    def put_current_page_on_page_stack(self):
        self._loaded_page_stack.append(self._loaded_page)

    def pop_last_page_stack(self):
        self._loaded_page = self._loaded_page_stack.pop(-1)

    def get_page_stack_size(self):
        return len(self._loaded_page_stack)

    def save_page(self):
        self._number_of_writes += 1
        with open(self._filename, "r+b") as file:
            file.seek(0)
            change = self._page_size * self._loaded_page.page_number
            file.seek(change)
            records_written = 0
            for metadata, pointer in zip(self._loaded_page.metadata_entries, self._loaded_page.pointer_entries):
                # First write pointer
                if pointer is None or self._loaded_page.is_leaf or records_written > self._loaded_page.keys_count:
                    file.write(sys.maxsize.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                else:
                    file.write(pointer.file_position.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                if metadata is None or records_written >= self._loaded_page.keys_count:
                    file.write(sys.maxsize.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                    file.write(sys.maxsize.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                else:
                    file.write(metadata.index.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                    file.write(metadata.page_number.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                    records_written += 1
            pointer = self._loaded_page.pointer_entries[-1]
            if pointer is None:
                file.write(sys.maxsize.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
            else:
                file.write(pointer.file_position.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))

    def get_records_page_number(self, index):
        page_number = self._loaded_page.get_records_page_number(index)
        return page_number

    def get_number_of_records(self):
        return self._loaded_page.keys_count

    def add_record(self, index, page_number):
        # When calling that make sure that there's place on this page!
        self._loaded_page.add_metadata_entry_between(IndexFilePageRecordEntry(index, page_number))

    def add_new_page(self, is_leaf):
        self._loaded_page = IndexFilePage(self._page_size_in_records, self._number_of_pages, is_leaf)
        self._number_of_pages += 1

    @property
    def loaded_page(self):
        return self._loaded_page

    @loaded_page.setter
    def loaded_page(self, value):
        self._loaded_page = value


class IndexFilePage:
    def __init__(self, page_size, page_number, is_leaf):
        self._metadata_entries: [IndexFilePageRecordEntry] = [None for _ in range(page_size)]
        self._pointer_entries: [IndexFilePageAddressEntry] = [None for _ in range(page_size + 1)]
        self._page_size = page_size
        self._page_number = page_number
        self._keys_count = 0
        self._is_leaf = is_leaf

    def fill(self):
        while len(self._metadata_entries) < self._page_size:
            self._metadata_entries.append(None)
        while len(self._pointer_entries) < self._page_size + 1:
            self._pointer_entries.append(None)

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
        self._keys_count += 1

    def get_records_page_number(self, index):
        return next(
            (entry.page_number for entry in self._metadata_entries if entry is not None and entry.index == index), None)

    @property
    def page_number(self):
        return self._page_number

    @page_number.setter
    def page_number(self, new_page_number):
        self._page_number = new_page_number

    @property
    def metadata_entries(self):
        return self._metadata_entries

    @property
    def pointer_entries(self):
        return self._pointer_entries

    @property
    def keys_count(self):
        return self._keys_count

    @keys_count.setter
    def keys_count(self, new):
        self._keys_count = new

    @property
    def is_leaf(self):
        return self._is_leaf

    @is_leaf.setter
    def is_leaf(self, new):
        self._is_leaf = new


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
