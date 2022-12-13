import random

from btree import BTree


class ProgramCommand:
    ADD_RECORD = 'A'
    READ_RECORD = 'R'
    PRINT_TREE = 'P'
    REORGANISE = 'O'
    DELETE = 'D'
    UPDATE = 'U'
    QUIT = "Q"


class ProgramManager:
    def __init__(self):
        self._btree = None
        pass

    @staticmethod
    def display_menu():
        print(f"{ProgramCommand.ADD_RECORD} - ADD RECORD")
        print(f"{ProgramCommand.READ_RECORD} - READ RECORD")
        print(f"{ProgramCommand.PRINT_TREE} - PRINT TREE")
        print(f"{ProgramCommand.REORGANISE} - REORGANISE")
        print(f"{ProgramCommand.DELETE} - DELETE RECORD")
        print(f"{ProgramCommand.UPDATE} - UPDATE RECORD")
        print(f"{ProgramCommand.QUIT} - QUIT THE PROGRAM")

    def run_the_program(self):
        exit_program = False
        print("Welcome to the program that implements the BTree data structure with index file!\nPlease enter tree "
              "order:")
        btree_degree_string = input()
        try:
            btree_degree_int = int(btree_degree_string)
        except ValueError:
            print("Please enter a valid BTree degree next time! Bye!")
            return
        self._btree = BTree(btree_degree_int)
        while exit_program is False:
            self.display_menu()
            command = input()
            match command:
                case ProgramCommand.ADD_RECORD:
                    self.add_record_command()
                case ProgramCommand.READ_RECORD:
                    pass
                case ProgramCommand.PRINT_TREE:
                    self.print_tree_command()
                case ProgramCommand.REORGANISE:
                    self.reorganise_command()
                case ProgramCommand.DELETE:
                    self.delete_record_command()
                case ProgramCommand.UPDATE:
                    self.update_record_command()
                case ProgramCommand.QUIT:
                    exit_program = True

    def add_record_command(self):
        input_string = input("Please enter the record you would like to add in a format:\nKEY P(A) P(B) P(AUB) I.E. "
                             "INT FLOAT FLOAT FLOAT\n")
        numbers = input_string.split(" ")
        assert(len(numbers) == 1)  # assert (len(numbers) == 4)
        try:
            index = int(numbers[0])
            a_probability = random.random()  # float(numbers[1])
            b_probability = random.random()  # float(numbers[2])
            sum_probability = random.random()  # float(numbers[3])
            assert (0 <= a_probability <= 1.0)
            assert (0 <= b_probability <= 1.0)
            assert (0 <= sum_probability <= 1.0)
        except AssertionError:
            print("Wrong value entered!")
            return
        except ValueError:
            print("Wrong value entered!")
            return
        self._btree.add_record(index, a_probability, b_probability, sum_probability, 0)

    def read_record_command(self):
        input_string = input("Please enter index of record you would like to read.\n")
        try:
            index = int(input_string)
        except ValueError:
            print("Invalid index of record!\n")
            return
        self._btree.read_record(index)

    def print_tree_command(self):
        self._btree.print_tree()

    def reorganise_command(self):
        self._btree.reorganise()

    def delete_record_command(self):
        input_string = input("Please enter index of the record you would like to delete.\n")
        try:
            index = int(input_string)
        except ValueError:
            print("Invalid index of record!\n")
            return
        self._btree.delete_record(index)

    def update_record_command(self):
        old_index = input("Please enter the old index of the record.\n")
        input_string = input("Please enter the new data of the record you would like to update in a format:\nKEY P("
                             "A) P(B) P(A∪B) I.E. INT FLOAT FLOAT FLOAT\n")
        numbers = input_string.split(" ")
        assert (len(numbers) == 4)
        try:
            index = int(numbers[0])
            a_probability = random.random()  # float(numbers[1])
            b_probability = random.random()  # float(numbers[2])
            sum_probability = random.random()  # float(numbers[3])
            assert (0 <= a_probability <= 1.0)
            assert (0 <= b_probability <= 1.0)
            assert (0 <= sum_probability <= 1.0)
        except AssertionError:
            print("Wrong value entered!")
            return
        self._btree.update_record(old_index, index, a_probability, b_probability, sum_probability)
