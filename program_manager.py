import os
import random

from btree import BTree
from contants import Constants
from validator import Validator


class ProgramCommand:
    ADD_RECORD = 'A'
    READ_RECORD = 'R'
    PRINT_TREE = 'P'
    DELETE = 'D'
    UPDATE = 'U'
    QUIT = "Q"
    LOAD = "L"
    VALIDATE = "V"
    FILE_ADD = "INSERT"
    FILE_PRINT = "PRINT"
    FILE_DELETE = "DELETE"


class ProgramManager:
    def __init__(self):
        self._btree = None
        self._validator = None
        self._d = None
        pass

    @staticmethod
    def display_menu():
        print(f"{ProgramCommand.ADD_RECORD} - ADD RECORD")
        print(f"{ProgramCommand.READ_RECORD} - READ RECORD")
        print(f"{ProgramCommand.PRINT_TREE} - PRINT TREE")
        print(f"{ProgramCommand.DELETE} - DELETE RECORD")
        print(f"{ProgramCommand.UPDATE} - UPDATE RECORD")
        print(f"{ProgramCommand.QUIT} - QUIT THE PROGRAM")
        print(f"{ProgramCommand.LOAD} - LOAD COMMANDS FROM FILE")

    def run_the_program(self):
        try:
            os.remove(Constants.RECORDS_FILENAME)
        except FileNotFoundError:
            pass
        try:
            os.remove(Constants.INDEXES_FILENAME)
        except FileNotFoundError:
            pass
        open(Constants.INDEXES_FILENAME, "w+").close()
        open(Constants.RECORDS_FILENAME, "w+").close()
        exit_program = False
        print("Welcome to the program that implements the BTree data structure with index file!\nPlease enter tree "
              "order:")
        btree_degree_string = input()
        try:
            btree_degree_int = int(btree_degree_string)
        except ValueError:
            print("Please enter a valid BTree degree next time! Bye!")
            return
        self._d = btree_degree_int
        self._btree = BTree(btree_degree_int)
        self._validator = Validator()
        while exit_program is False:
            self.display_menu()
            command = input()
            match command:
                case ProgramCommand.ADD_RECORD:
                    self.add_record_command()
                case ProgramCommand.READ_RECORD:
                    self.read_record_command()
                case ProgramCommand.PRINT_TREE:
                    self.print_tree_command()
                case ProgramCommand.DELETE:
                    self.delete_record_command()
                case ProgramCommand.UPDATE:
                    self.update_record_command()
                case ProgramCommand.QUIT:
                    exit_program = True
                case ProgramCommand.LOAD:
                    self.load_command()
                case ProgramCommand.VALIDATE:
                    self.validate()

    def add_record_command(self):
        input_string = input("Please enter the record you would like to add in a format:\nKEY P(A) P(B) P(AUB) I.E. "
                             "INT FLOAT FLOAT FLOAT\n")
        numbers = input_string.split(" ")
        assert (len(numbers) == 1)  # assert (len(numbers) == 4)
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
        record = self._btree.read_record(index, True)
        if record is not None:
            print(record.serialize())
        else:
            print("Such record doesn't exist!")

    def print_tree_command(self):
        self._btree.print_tree()

    def delete_record_command(self):
        input_string = input("Please enter index of the record you would like to delete.\n")
        try:
            index = int(input_string)
        except ValueError:
            print("Invalid index of record!\n")
            return
        self._btree.delete_record(index, 0)

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

    def load_command(self, filename_arg=""):
        # Load sequence of commands from the file
        if filename_arg == "":
            filename = input("Please enter filename of the file with commands: ")
            try:
                open(filename, "r").close()
            except FileNotFoundError:
                print("Such file doesn't exist")
        else:
            filename = filename_arg
        with open(filename, "r") as f:
            while True:
                line = f.readline()
                if not line:
                    break
                line = line.rstrip()
                line_arr = line.split(Constants.FILE_DELIMITER)
                cmd = line_arr[0]
                if cmd == ProgramCommand.FILE_ADD:
                    print(f"INSERT {int(line_arr[1])}")
                    # self._btree.add_record(int(line_arr[1]), float(line_arr[2]), float(line_arr[3]), float(line_arr[4]), 0)
                    self._btree.add_record(int(line_arr[1]), random.random(), random.random(), random.random(), 0)
                elif cmd == ProgramCommand.FILE_PRINT:
                    self._btree.print_tree()
                elif cmd == ProgramCommand.FILE_DELETE:
                    print(f"DELETE {int(line_arr[1])}")
                    self._btree.delete_record(line_arr[1])

    def validate(self):
        cnt = int(input("How many records to test?"))
        self._btree = BTree(self._d)
        self._validator.add_x_records(cnt)
        self.load_command(Constants.VALIDATOR_FILENAME)
