from index_file_handler import IndexFileHandler
from record_file_handler import RecordFileHandler


class BTree:
    def __init__(self, degree=2):
        self._d = degree
        self._max_keys = 2 * degree
        self._index_file = IndexFileHandler(self._max_keys)
        self._record_file = RecordFileHandler(self._max_keys)

    def add_record(self, index, a_probability, b_probability, sum_probability):
        pass

    def read_record(self, index):
        pass

    def print_tree(self):
        pass

    def reorganise(self):
        pass

    def delete(self, index):
        pass

    def update(self, index, a_probability, b_probability, sum_probability):
        pass
