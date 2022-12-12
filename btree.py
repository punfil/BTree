from index_file_handler import IndexFileHandler
from record_file_handler import RecordFileHandler


class BTree:
    def __init__(self, degree=2):
        self._d = degree
        self._max_keys = 2 * degree
        self._index_file = IndexFileHandler(2 * degree)
        self._record_file = RecordFileHandler(2 * degree)

    def add_record(self, index, a_probability, b_probability, sum_probability):
        pass

    def read_record(self, index):
        page_number = self._index_file.get_records_page_number(index)
        record = self._record_file.get_record_by_index(index, page_number)
        print(record.serialize())

    def print_tree(self):
        pass

    def reorganise(self):
        pass

    def delete_record(self, index):
        pass

    def update_record(self, old_index, index, a_probability, b_probability, sum_probability):
        self.delete_record(old_index)
        self.add_record(index, a_probability, b_probability, sum_probability)
