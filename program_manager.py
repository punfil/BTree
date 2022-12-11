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

    def run_the_program(self):
        exit_program = False
        print("Welcome to the program that implements the BTree data structure with index file!\n Please enter tree "
              "order:\n")
        btree_degree_string = input()
        try:
            btree_degree_int = int(btree_degree_string)
        except ValueError:
            print("Please enter a valid BTree degree next time! Bye!\n")
            return
        self._btree = BTree(btree_degree_int)
        while exit_program is False:
            command = input()
            match command:
                case ProgramCommand.ADD_RECORD:
                    self.add_record_command()
                case ProgramCommand.READ_RECORD:
                    pass
                case ProgramCommand.PRINT_TREE:
                    pass
                case ProgramCommand.REORGANISE:
                    pass
                case ProgramCommand.DELETE:
                    self.delete_record_command()
                case ProgramCommand.UPDATE:
                    pass
                case ProgramCommand.QUIT:
                    exit_program = True

    def add_record_command(self):
        input_string = input("Please enter the record you would like to add in a format:\n KEY P(A) P(B) P(A∪B) I.E. "
                             "INT FLOAT FLOAT FLOAT\n")
        numbers = input_string.split(" ")
        assert (len(numbers) == 4)
        try:
            index = int(numbers[0])
            a_probability = float(numbers[1])
            b_probability = float(numbers[2])
            sum_probability = float(numbers[3])
            assert (0 <= a_probability <= 1.0)
            assert (0 <= b_probability <= 1.0)
            assert (0 <= sum_probability <= 1.0)
        except ValueError:
            print("Wrong value entered!")
            return
        self._btree.add_record(index, a_probability, b_probability, sum_probability)

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
