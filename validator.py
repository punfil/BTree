import random

from contants import Constants


class Validator:
    def __init__(self):
        self._filename = Constants.VALIDATOR_FILENAME

    def add_x_records(self, x):
        numbers = [i for i in range(x)]
        with open(self._filename, "w+") as f:
            while len(numbers):
                number = random.choice(numbers)
                numbers.remove(number)
                # f.write(f"INSERT {number} {random.random()} {random.random()} {random.random()}")
                f.write(f"INSERT {number}\n")
                f.write(f"PRINT\n")
