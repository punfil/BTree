from index_file_handler import IndexFileHandler
from record_file_handler import RecordFileHandler


class Node:
    def __init__(self, degree, is_leaf=True):
        self._degree = degree
        self._index_file = IndexFileHandler(2 * degree)
        self._record_file = RecordFileHandler(2 * degree)  # This will be used only in root node
        self._is_leaf = is_leaf

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

    @property
    def index_file(self):
        return self._index_file

    @property
    def record_file(self):
        return self._record_file
