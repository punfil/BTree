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
        if self._index_file.get_number_of_records():
            print("(", end="")
        for i in range(self._index_file.get_number_of_records()):
            if self._is_leaf is False:
                # sons[i]->print_tree()
                node = Node(self._degree)
                node._index_file.load_page(self._index_file.loaded_page.get_particular_pointer_entry(i))
                node.print_tree()
            # print(keys[i])
            record = self._index_file.loaded_page.get_particular_metadata_entry(i)
            print(self._record_file.get_record_by_index(record.index, record.page_number))
        if self._is_leaf is False:
            node = Node(self._degree)
            node._index_file.load_page(self._index_file.loaded_page.get_particular_pointer_entry(-1))
            node.print_tree()
        if self._index_file.get_number_of_records():
            print(")", end="")

    def reorganise(self):
        pass

    def delete_record(self, index):
        pass

    def update_record(self, old_index, index, a_probability, b_probability, sum_probability):
        self.delete_record(old_index)
        self.add_record(index, a_probability, b_probability, sum_probability)
