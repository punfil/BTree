from index_file_handler import IndexFileHandler
from record_file_handler import RecordFileHandler
from itertools import zip_longest

class BTree:
    def __init__(self, degree=2):
        self._d = degree
        self._max_keys = 2 * degree
        self._index_file = IndexFileHandler(2 * degree)
        self._record_file = RecordFileHandler(2 * degree)

    def add_record(self, index, a_probability, b_probability, sum_probability):
        # Create this record entry in record table
        page_number = self._record_file.add_record(index, a_probability, b_probability, sum_probability)
        # Check if root has ability to insert directly
        if self._index_file.get_number_of_records() < self._max_keys:
            # There's place for this record
            self._index_file.add_record(index, page_number)
        else:
            pass

    def read_record(self, index):
        pass

    def print_tree(self):
        print("( ", end="")
        for son, record in zip_longest(self._index_file.loaded_page.pointer_entries, self._index_file.loaded_page.metadata_entries):
            if son is not None:
                self._index_file.put_current_page_on_page_stack()
                self._index_file.load_page(son.file_position)
                self.print_tree()
                self._index_file.pop_last_page_stack()
            if record is not None:  # Always when node is not a leaf
                print(self._record_file.get_record_by_index(record.index, record.page_number).serialize(), end=" ")
        print(")", end="")

    def reorganise(self):
        pass

    def delete_record(self, index):
        pass

    def update_record(self, old_index, index, a_probability, b_probability, sum_probability):
        self.delete_record(old_index)
        self.add_record(index, a_probability, b_probability, sum_probability)
