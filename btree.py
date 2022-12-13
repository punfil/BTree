from index_file_handler import *
from record_file_handler import RecordFileHandler
from itertools import zip_longest


class BTree:
    def __init__(self, degree=2):
        self._d = degree
        self._max_keys = 2 * degree
        self._index_file = IndexFileHandler(2 * degree)
        self._record_file = RecordFileHandler(2 * degree)

    def add_record(self, index, a_probability, b_probability, sum_probability):
        # Check if root has ability to insert directly
        if self._index_file.get_number_of_records() < self._max_keys:
            # There's place for this record
            page_number = self._record_file.add_record(index, a_probability, b_probability, sum_probability)
            self._index_file.add_record(index, page_number)
        else:
            current_page = self._index_file.loaded_page
            self._index_file.add_new_page()
            son_page = self._index_file.loaded_page
            # They will be switched places anyway, that's why son_page.page_number not current_page
            son_page.pointer_entries[0] = IndexFilePageAddressEntry(current_page.page_number)
            self.split_child(0, current_page)
            # Switch places in real file
            current_page.page_number, son_page.page_number = son_page.page_number, current_page.page_number
            # Save new parent page after split_child
            self._index_file.save_page()
            # Save old parent page after split_child
            self._index_file.loaded_page = current_page
            self._index_file.save_page()
            # Emulation of recursive execution on the new son
            self._index_file.loaded_page = son_page
            self.add_record(index, a_probability, b_probability, sum_probability)

    def split_child(self, i, old_root):
        new_root = self._index_file.loaded_page
        self._index_file.add_new_page()
        temp = self._index_file.loaded_page
        self._index_file.loaded_page = new_root
        for j in range(0, self._d - 1):
            temp.metadata_entries[j] = old_root.metadata_entries[j + self._d]
        if old_root.get_number_of_pointer_entries():
            for j in range(0, self._d):
                temp.pointer_entries[j] = old_root.pointer_entries[j + self._d]
        for j in range(new_root.get_number_of_metadata_entries(), i, -1):
            new_root.pointer_entries[j + 1] = new_root.pointer_entries[j]
        new_root.pointer_entries[i + 1] = IndexFilePageAddressEntry(temp.page_number)
        for j in range(self._d - 1, i - 1, -1):
            new_root.metadata_entries[j + 1] = new_root.metadata_entries[j]
        new_root.metadata_entries[i] = old_root.metadata_entries[self._d - 1]
        self._index_file.save_page()
        self._index_file = new_root

    def read_record(self, index):
        pass

    def print_tree(self):
        print("( ", end="")
        for son, record in zip_longest(self._index_file.loaded_page.pointer_entries,
                                       self._index_file.loaded_page.metadata_entries):
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
