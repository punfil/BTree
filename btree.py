from index_file_handler import *
from record_file_handler import RecordFileHandler
from itertools import zip_longest


class BTree:
    def __init__(self, degree=2):
        self._d = degree
        self._max_keys = 2 * degree
        self._index_file = IndexFileHandler(2 * degree)
        self._record_file = RecordFileHandler(2 * degree)

    @staticmethod
    def print_io_operations(reads, writes):
        print(f"During operation reads: {reads} writes: {writes}")

    def add_record(self, index, a_probability, b_probability, sum_probability, recurrency_depth):
        if recurrency_depth == 0:
            self._index_file.clear_io_operations_counters()
            self._record_file.clear_io_operations_counters()
            if self.read_record(index) is not None:
                ireads, iwrites = self._index_file.get_io_operations()
                rreads, rwrites = self._index_file.get_io_operations()
                self.print_io_operations(ireads + rreads, iwrites + rwrites)
                return

        if recurrency_depth == 0 and self._index_file.loaded_page.keys_count == 2 * self._d:
            # From btree.cpp
            current_page = self._index_file.loaded_page

            self._index_file.add_new_page(is_leaf=False)
            son_page = self._index_file.loaded_page

            # They will be switched places anyway, that's why son_page.page_number not current_page
            son_page.pointer_entries[0] = IndexFilePageAddressEntry(son_page.page_number)

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

            self.add_record(index, a_probability, b_probability, sum_probability, recurrency_depth + 1)
        else:
            # from node.cpp
            i = self._index_file.loaded_page.keys_count - 1
            if self._index_file.loaded_page.is_leaf is True:

                page_number = self._record_file.add_record(index, a_probability, b_probability, sum_probability)

                while i >= 0 and index < self._index_file.loaded_page.metadata_entries[i].index:
                    self._index_file.loaded_page.metadata_entries[i + 1] = \
                        self._index_file.loaded_page.metadata_entries[i]
                    i -= 1

                self._index_file.loaded_page.metadata_entries[i + 1] = IndexFilePageRecordEntry(index, page_number)
                self._index_file.loaded_page.keys_count += 1
            else:
                while i >= 0 and self._index_file.loaded_page.metadata_entries[i].index > index:
                    i -= 1

                i += 1

                old_parent = self._index_file.loaded_page
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i].file_position)
                ison = self._index_file.loaded_page
                self._index_file.loaded_page = old_parent

                if ison.keys_count == 2 * self._d:
                    self.split_child(i, ison)
                    if index > old_parent.metadata_entries[i].index:
                        i += 1
                self._index_file.save_page()
                self._index_file.loaded_page = ison
                self._index_file.save_page()
                self._index_file.loaded_page = old_parent

                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i].file_position)

                self.add_record(index, a_probability, b_probability, sum_probability, recurrency_depth + 1)
                self._index_file.save_page()
                self._index_file.loaded_page = old_parent
        if recurrency_depth == 0:
            ireads, iwrites = self._index_file.get_io_operations()
            rreads, rwrites = self._index_file.get_io_operations()
            self.print_io_operations(ireads + rreads, iwrites + rwrites)

    def split_child(self, i, old_root):
        # this should be loaded as loaded_page

        new_root = self._index_file.loaded_page
        self._index_file.add_new_page(old_root.is_leaf)
        temp = self._index_file.loaded_page

        temp.keys_count = self._d
        self._index_file.loaded_page = new_root

        for j in range(0, self._d):
            temp.metadata_entries[j] = old_root.metadata_entries[j + self._d]

        if not old_root.is_leaf:
            for j in range(0, self._d + 1):
                temp.pointer_entries[j] = old_root.pointer_entries[j + self._d]

        old_root.keys_count = self._d - 1

        for j in range(new_root.keys_count, i, -1):
            new_root.pointer_entries[j + 1] = new_root.pointer_entries[j]

        new_root.pointer_entries[i + 1] = IndexFilePageAddressEntry(temp.page_number)

        for j in range(self._d - 1, i - 1, -1):
            new_root.metadata_entries[j + 1] = new_root.metadata_entries[j]

        new_root.metadata_entries[i] = old_root.metadata_entries[self._d - 1]

        new_root.keys_count += 1

        self._index_file.loaded_page = temp
        self._index_file.save_page()
        self._index_file.loaded_page = new_root

    def read_record(self, index):
        if self._index_file.get_page_stack_size() == 0:
            self._index_file.clear_io_operations_counters()
            self._record_file.clear_io_operations_counters()

        if not self._index_file.loaded_page.keys_count:
            return None

        i = self._index_file.loaded_page.keys_count - 1
        while i > 0 and index < self._index_file.loaded_page.metadata_entries[i].index:
            i -= 1
        if self._index_file.loaded_page.metadata_entries[i].index == index:
            return self._record_file.get_record_by_index(self._index_file.loaded_page.metadata_entries[i].index,
                                                         self._index_file.loaded_page.metadata_entries[i].page_number)
        elif self._index_file.loaded_page.is_leaf is False:
            self._index_file.save_page()
            self._index_file.put_current_page_on_page_stack()
            if index < self._index_file.loaded_page.metadata_entries[i].index:
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i].file_position)
            else:
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i + 1].file_position)
            record = self.read_record(index)
            self._index_file.pop_last_page_stack()

            ireads, iwrites = self._index_file.get_io_operations()
            rreads, rwrites = self._index_file.get_io_operations()
            self.print_io_operations(ireads + rreads, iwrites + rwrites)

            return record
        else:
            ireads, iwrites = self._index_file.get_io_operations()
            rreads, rwrites = self._index_file.get_io_operations()
            self.print_io_operations(ireads + rreads, iwrites + rwrites)
            return None

    def print_tree(self):
        if self._index_file.get_page_stack_size() == 0:
            self._index_file.clear_io_operations_counters()
            self._record_file.clear_io_operations_counters()

        print("( ", end="")
        a = self._index_file.loaded_page.keys_count
        for i in range(0, self._index_file.loaded_page.keys_count):
            # if is_leaf == false
            if not self._index_file.loaded_page.is_leaf:
                self._index_file.put_current_page_on_page_stack()
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i].file_position)
                self.print_tree()
                self._index_file.pop_last_page_stack()
            print(self._record_file.get_record_by_index(self._index_file.loaded_page.metadata_entries[i].index,
                                                        self._index_file.loaded_page.metadata_entries[
                                                            i].page_number).serialize(), end=" ")
        if not self._index_file.loaded_page.is_leaf != 0:
            self._index_file.put_current_page_on_page_stack()
            self._index_file.load_page(self._index_file.loaded_page.pointer_entries[a].file_position)
            self.print_tree()
            self._index_file.pop_last_page_stack()
        print(") ", end="")

        if self._index_file.get_page_stack_size() == 0:
            print("")  # Enter at the end
            ireads, iwrites = self._index_file.get_io_operations()
            rreads, rwrites = self._record_file.get_io_operations()
            self.print_io_operations(ireads + rreads, iwrites + rwrites)

    def delete_record(self, index):
        pass

    def update_record(self, old_index, index, a_probability, b_probability, sum_probability):
        if self.read_record(index) is None:
            self.delete_record(old_index)
            self.add_record(index, a_probability, b_probability, sum_probability)
        else:
            print("Record with given new index already exists!")
