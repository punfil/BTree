from index_file_handler import *
from record_file_handler import RecordFileHandler


class BTree:
    def __init__(self, degree=2):
        self._d = degree
        self._max_keys = 2 * degree
        self._index_file = IndexFileHandler(2 * degree)
        self._record_file = RecordFileHandler(2 * degree)

    @staticmethod
    def print_io_operations(reads, writes):
        print(f"During operation reads: {reads} writes: {writes}")

    @staticmethod
    def compensate(left_node, right_node, parent, i, new_record):
        # Only for leafs
        left_node.keys_count = 0
        right_node.keys_count = 0
        records_list = []
        [records_list.append(x) for x in left_node.metadata_entries if x is not None]
        records_list.append(parent.metadata_entries[i])
        [records_list.append(x) for x in right_node.metadata_entries if x is not None]
        records_list.append(new_record)
        records_list.sort()
        pointers_list = []
        [pointers_list.append(x) for x in left_node.pointer_entries if x is not None]
        [pointers_list.append(x) for x in right_node.pointer_entries if x is not None]
        partition = len(records_list) // 2

        for j in range(partition):
            left_node.metadata_entries[j] = records_list[j]
            left_node.keys_count += 1
        for j in range(partition + 1, len(records_list)):
            right_node.metadata_entries[j - partition - 1] = records_list[j]
            right_node.keys_count += 1

        parent.metadata_entries[i] = records_list[partition]
        if left_node.is_leaf is not True:
            for j in range(partition + 1):
                left_node.pointer_entries[j] = pointers_list[j]
            for j in range(partition + 1, len(pointers_list)):
                right_node.pointer_entries[j - partition - 1] = pointers_list[j]

    def add_record(self, index, a_probability, b_probability, sum_probability, recurrency_depth):
        if recurrency_depth == 0:  # This means add_record is called on root
            self._index_file.clear_io_operations_counters()
            self._record_file.clear_io_operations_counters()
            if self.read_record(index) is not None:  # Such record already exists
                ireads, iwrites = self._index_file.get_io_operations()
                rreads, rwrites = self._index_file.get_io_operations()
                print("Such record already exists!")
                self.print_io_operations(ireads + rreads, iwrites + rwrites)
                return

        # If we are on root we need to split it.
        if recurrency_depth == 0 and self._index_file.loaded_page.keys_count == 2 * self._d:
            current_page = self._index_file.loaded_page
            # Add new page for first son of the root
            self._index_file.add_new_page(is_leaf=False)
            son_page = self._index_file.loaded_page

            # They will be switched places anyway, that's why son_page.page_number not current_page 2 commands below
            son_page.pointer_entries[0] = IndexFilePageAddressEntry(son_page.page_number)
            # Perform split child
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
            i = self._index_file.loaded_page.keys_count - 1
            if self._index_file.loaded_page.is_leaf is True:
                # If we are in leaf it's ok to add it here. We know we won't overflow, because in keys_count == 2*d
                # scenario split would happen before entering son
                page_number = self._record_file.add_record(index, a_probability, b_probability, sum_probability)
                # Find the first number that is not greater than index
                while i >= 0 and index < self._index_file.loaded_page.metadata_entries[i].index:
                    self._index_file.loaded_page.metadata_entries[i + 1] = \
                        self._index_file.loaded_page.metadata_entries[i]
                    i -= 1
                # Add element at that index
                self._index_file.loaded_page.metadata_entries[i + 1] = IndexFilePageRecordEntry(index, page_number)
                self._index_file.loaded_page.keys_count += 1
                # There is no need to save that page, because either it's root or it will be saved
                # as it's recurrent func call.
            else:
                # Find the first record that is not greater then index
                while i >= 0 and self._index_file.loaded_page.metadata_entries[i].index > index:
                    i -= 1
                # len(sons) = len(keys) +1 in non-leaf nodes
                i += 1
                # Load sons[i]
                old_parent = self._index_file.loaded_page
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i].file_position)
                ison = self._index_file.loaded_page

                compensation_done = False
                if ison.keys_count == 2 * self._d:
                    # No place in the sons[i].
                    # Try left compensation
                    if i > 0 and ison.is_leaf is True:
                        # Load left sibling of sons[i]
                        self._index_file.load_page(old_parent.pointer_entries[i - 1].file_position)
                        left_sibling = self._index_file.loaded_page
                        if self._index_file.loaded_page.keys_count < 2 * self._d:
                            # There's place in the left sibling. We can compensate
                            page_number = self._record_file.add_record(index, a_probability, b_probability,
                                                                       sum_probability)
                            self.compensate(left_sibling, ison, old_parent, i - 1,
                                            IndexFilePageRecordEntry(index, page_number))
                            compensation_done = True  # Record has been added.
                    # Try right compensation if left has not been done.
                    if compensation_done is False and ison.is_leaf is True and i < old_parent.keys_count - 1:
                        self._index_file.load_page(old_parent.pointer_entries[i + 1].file_position)
                        right_sibling = self._index_file.loaded_page
                        if self._index_file.loaded_page.keys_count < 2 * self._d:
                            # There's place in the right sibling. We can compesante.
                            page_number = self._record_file.add_record(index, a_probability, b_probability,
                                                                       sum_probability)
                            self.compensate(ison, right_sibling, old_parent, i,
                                            IndexFilePageRecordEntry(index, page_number))
                            self._index_file.loaded_page = right_sibling
                            compensation_done = True  # Record has been added.
                    # Compensation impossible. We need to split the sons[i] node.
                    if compensation_done is False:
                        self._index_file.loaded_page = old_parent
                        self.split_child(i, ison)
                        if index > old_parent.metadata_entries[i].index:
                            i += 1
                    self._index_file.save_page()  # Save the new sibling after operation
                    self._index_file.loaded_page = ison
                    self._index_file.save_page()  # Save the sons[i] after operation
                # It will be saved in a recurrent call
                # or if it's root it's not required
                self._index_file.loaded_page = old_parent
                if compensation_done is True:
                    return  # Record added, not further action required
                # Load the new sons[i] after split and try to do the same.
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i].file_position)
                self.add_record(index, a_probability, b_probability, sum_probability, recurrency_depth + 1)
                self._index_file.save_page()
                self._index_file.loaded_page = old_parent
        if recurrency_depth == 0:  # If we are back in root after recurrent calls we can print stats.
            ireads, iwrites = self._index_file.get_io_operations()
            rreads, rwrites = self._index_file.get_io_operations()
            self.print_io_operations(ireads + rreads, iwrites + rwrites)

    def split_child(self, i, old_root):
        # The new left son should be loaded before calling this function
        new_root = self._index_file.loaded_page
        # Create new right sibling of the new left son
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

        for j in range(new_root.keys_count - 1, i - 1, -1):
            new_root.metadata_entries[j + 1] = new_root.metadata_entries[j]

        new_root.metadata_entries[i] = old_root.metadata_entries[self._d - 1]

        new_root.keys_count += 1

        self._index_file.loaded_page = temp
        self._index_file.save_page()
        self._index_file.loaded_page = new_root

    def read_record(self, index, check_io=False):
        if self._index_file.get_page_stack_size() == 0 and check_io:
            self._index_file.clear_io_operations_counters()
            self._record_file.clear_io_operations_counters()

        if not self._index_file.loaded_page.keys_count:
            return None

        i = self._index_file.loaded_page.keys_count - 1
        while i > 0 and index < self._index_file.loaded_page.metadata_entries[i].index:
            i -= 1
        if self._index_file.loaded_page.metadata_entries[i].index == index:
            returning = self._record_file.get_record_by_index(self._index_file.loaded_page.metadata_entries[i].index,
                                                              self._index_file.loaded_page.metadata_entries[
                                                                  i].page_number)
        elif self._index_file.loaded_page.is_leaf is False:
            self._index_file.save_page()
            self._index_file.put_current_page_on_page_stack()
            if index < self._index_file.loaded_page.metadata_entries[i].index:
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i].file_position)
            else:
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[i + 1].file_position)
            record = self.read_record(index)
            self._index_file.pop_last_page_stack()

            returning = record
        else:
            returning = None
        if check_io:
            ireads, iwrites = self._index_file.get_io_operations()
            rreads, rwrites = self._index_file.get_io_operations()
            self.print_io_operations(ireads + rreads, iwrites + rwrites)
        return returning

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

    @staticmethod
    def greater_or_equal(index, node: IndexFilePage):
        i = 0
        while i < node.keys_count and node.metadata_entries[i].index < index:
            i += 1
        return i

    def remove_from_leaf(self, index):
        # Run this method with page loaded on stack
        node = self._index_file.loaded_page
        for i in range(index + 1, node.keys_count):
            node.metadata_entries[i - 1] = node.metadata_entries[i]
        node.keys_count -= 1

    def remove_from_non_leaf(self, index, recurrency_depth):
        # Run this method with page loaded on stack
        node = self._index_file.loaded_page
        number = node.metadata_entries[index].index

        self._index_file.load_page(node.pointer_entries[index].file_position)
        ison = self._index_file.loaded_page
        self._index_file.load_page(node.pointer_entries[index + 1].file_position)
        i1son = self._index_file.loaded_page
        if ison.keys_count >= self._d: # Edit ?
            current = ison
            while not current.is_leaf:
                self._index_file.load_page(current.pointer_entries[current.keys_count].file_position)
                current = self._index_file.loaded_page
            predecessor = current.metadata_entries[current.keys_count - 1]
            node.metadata_entries[index] = predecessor
            self._index_file.loaded_page = ison
            self.delete_record(predecessor.index, recurrency_depth + 1)  # Fix this
            self._index_file.save_page()
            self._index_file.loaded_page = node
            self._index_file.save_page()
        elif i1son.keys_count >= self._d: #Edit ?
            current = i1son
            while not current.is_leaf:
                self._index_file.load_page(current.pointer_entries[0].file_position)
                current = self._index_file.loaded_page
            successor = current.metadata_entries[0]
            node.metadata_entries[index] = successor
            self._index_file.loaded_page = i1son
            self.delete_record(successor.index, recurrency_depth + 1)
            self._index_file.save_page()
            self._index_file.loaded_page = ison
            self._index_file.save_page()
            self._index_file.loaded_page = node
            self._index_file.save_page()
        else:
            self._index_file.loaded_page = node
            ison = self.merge_keys(index)
            self._index_file.save_page()
            self._index_file.loaded_page = ison
            self.delete_record(number, recurrency_depth + 1)
            self._index_file.save_page()
            self._index_file.loaded_page = node
            self._index_file.save_page()

    def merge_keys(self, index):
        # Run this method with page loaded on stack
        node = self._index_file.loaded_page
        self._index_file.load_page(node.pointer_entries[index].file_position)
        child = self._index_file.loaded_page
        self._index_file.load_page(node.pointer_entries[index + 1].file_position)
        sibling = self._index_file.loaded_page
        child.metadata_entries[self._d-1] = node.metadata_entries[index]
        for i in range(sibling.keys_count):
            child.metadata_entries[i + self._d] = sibling.metadata_entries[i]
        if not child.is_leaf:
            for i in range(sibling.keys_count + 1):
                child.pointer_entries[i + self._d] = sibling.pointer_entries[i]
        for i in range(index + 1, node.keys_count):
            node.metadata_entries[i - 1] = node.metadata_entries[i]
        for i in range(index + 2, node.keys_count + 1):
            node.pointer_entries[i - 1] = node.pointer_entries[i]
        child.keys_count += sibling.keys_count + 1
        node.keys_count -= 1
        sibling.keys_count = 0  # Delete
        self._index_file.save_page()
        self._index_file.loaded_page = child
        self._index_file.save_page()
        self._index_file.loaded_page = node
        return child

    def fill_the_child(self, index):
        # Run this method with page loaded on stack
        node = self._index_file.loaded_page
        self._index_file.load_page(node.pointer_entries[index].file_position)
        ison = self._index_file.loaded_page
        self._index_file.loaded_page = node
        done = False
        if index != 0:
            self._index_file.load_page(node.pointer_entries[index - 1].file_position)
            ison_prev = self._index_file.loaded_page
            self._index_file.loaded_page = node
            if ison_prev.keys_count >= self._d:
                self.borrow_from_previous(index, node, ison, ison_prev)
                self._index_file.loaded_page = ison_prev
                self._index_file.save_page()
                self._index_file.loaded_page = node
                done = True
        if index != node.keys_count and not done:
            self._index_file.load_page(node.pointer_entries[index + 1].file_position)
            ison_next = self._index_file.loaded_page
            self._index_file.loaded_page = node
            if ison_next.keys_count >= self._d:
                self.borrow_from_next(index, node, ison, ison_next)
                self._index_file.loaded_page = ison_next
                self._index_file.save_page()
                self._index_file.loaded_page = node
                self._index_file.save_page()
                done = True
        if index != node.keys_count and not done:
            ison = self.merge_keys(index)
        elif not done:
            ison = self.merge_keys(index - 1)
        self._index_file.loaded_page = ison
        self._index_file.save_page()

    @staticmethod
    def borrow_from_previous(index, parent, child, sibling):
        for i in range(child.keys_count - 1, -1, -1):
            child.metadata_entries[i + 1] = child.metadata_entries[i]
        if not child.is_leaf:
            for i in range(child.keys_count, -1, -1):
                child.pointer_entries[i + 1] = child.pointer_entries[i]
        child.metadata_entries[0] = parent.metadata_entries[index - 1]
        if not child.is_leaf:
            child.pointer_entries[0] = sibling.pointer_entries[sibling.keys_count]
        parent.metadata_entries[index - 1] = sibling.metadata_entries[sibling.keys_count - 1]
        child.keys_count += 1
        sibling.keys_count -= 1

    @staticmethod
    def borrow_from_next(index, parent, child, sibling):
        child.metadata_entries[child.keys_count] = parent.metadata_entries[index]
        if not child.is_leaf:
            child.pointer_entries[child.keys_count + 1] = sibling.pointer_entries[0]
        parent.metadata_entries[index] = sibling.metadata_entries[0]
        for i in range(1, sibling.keys_count):
            sibling.metadata_entries[i - 1] = sibling.metadata_entries[i]
        if not sibling.is_leaf:
            for i in range(1, sibling.keys_count + 1):
                sibling.pointer_entries[i - 1] = sibling.pointer_entries[i]
        child.keys_count += 1
        sibling.keys_count -= 1

    def delete_record(self, index, recurrency_depth):
        if recurrency_depth == 0:
            # We are on root node
            self._index_file.clear_io_operations_counters()
            self._record_file.clear_io_operations_counters()
            # Check if such record exists is not required as it's checked later anyway
            self.delete_record(index, recurrency_depth + 1)
            if self._index_file.loaded_page.keys_count == 0 and not self._index_file.loaded_page.is_leaf:
                parent = self._index_file.loaded_page
                self._index_file.load_page(self._index_file.loaded_page.pointer_entries[0].file_position)
                ison = self._index_file.loaded_page
                ison.page_number = parent.page_number
                self._index_file.save_page()
        else:
            # Perform removal in node
            i = self.greater_or_equal(index, self._index_file.loaded_page)
            if i < self._index_file.loaded_page.keys_count and self._index_file.loaded_page.metadata_entries[i].index == index:
                if self._index_file.loaded_page.is_leaf:
                    self.remove_from_leaf(i)
                else:
                    self.remove_from_non_leaf(i, recurrency_depth)
            else:
                if self._index_file.loaded_page.is_leaf:
                    # Such record doesn't exist!
                    print("Record requested to be deleted doesn't exist!")
                    return
                flag = False
                if self._index_file.loaded_page.keys_count == i:
                    flag = True

                parent = self._index_file.loaded_page
                self._index_file.load_page(parent.pointer_entries[i].file_position)
                ison = self._index_file.loaded_page
                if ison.keys_count < self._d: # Edit ?
                    self._index_file.loaded_page = parent
                    self.fill_the_child(i)
                if flag and i > parent.keys_count:
                    self._index_file.load_page(parent.pointer_entries[i - 1].file_position)
                self.delete_record(index, recurrency_depth + 1)
                self._index_file.save_page()
                self._index_file.loaded_page = parent

    def update_record(self, old_index, index, a_probability, b_probability, sum_probability):
        if self.read_record(index) is None:
            self.delete_record(old_index, 0)
            self.add_record(index, a_probability, b_probability, sum_probability, 0)
        else:
            print("Record with given new index already exists!")
