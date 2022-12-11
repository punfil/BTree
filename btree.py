from node import Node


class BTree:
    def __init__(self, degree=2):
        self._d = degree
        self._max_keys = 2 * degree
        self._root_node = Node(degree)

    def add_record(self, index, a_probability, b_probability, sum_probability):
        pass

    def read_record(self, index):
        pass

    def print_tree(self):
        self._root_node.print_tree()
        print("\n")

    def reorganise(self):
        pass

    def delete_record(self, index):
        pass

    def update_record(self, old_index, index, a_probability, b_probability, sum_probability):
        self.delete_record(old_index)
        self.add_record(index, a_probability, b_probability, sum_probability)
