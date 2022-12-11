class RecordFileHandler():
    def __init__(self):
        self._page = RecordFilePage
        self._filename = "records.txt"


class RecordFilePage:
    def __init__(self):
        self._records = list()


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
    def index(self):
        return self._index

    @property
    def page_number(self):
        return self._page_number