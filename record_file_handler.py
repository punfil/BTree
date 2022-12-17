import random
import struct
import sys
from os import path
from sys import maxsize

from contants import Constants
from record import Record


def binary_to_float(binary):
    return struct.unpack('d', binary)[0]


def float_to_binary(float_number):
    return struct.pack('d', float_number)


class RecordFileHandler:
    def __init__(self, page_size=4):  # Page size in number of records
        self._filename = Constants.RECORDS_FILENAME
        self._loaded_page = RecordFilePage(0)
        self._number_of_pages = 1
        # Index + P(A) + P(B) + P(AUB)
        self._page_size = page_size * (Constants.INTEGER_SIZE + 3 * Constants.FLOAT_SIZE)
        self._max_number_of_records = page_size
        self._number_of_reads = 0
        self._number_of_writes = 0

    def clear_io_operations_counters(self):
        self._number_of_reads = 0
        self._number_of_writes = 0

    def get_io_operations(self):
        return self._number_of_reads, self._number_of_writes

    def create_new_page(self):
        self._loaded_page.create_new_page(self._number_of_pages)
        self._number_of_pages += 1

    def load_existing_page(self, page_number):
        try:
            assert (path.exists(self._filename))
            assert (0 <= page_number < self._number_of_pages)
        except AssertionError:
            return
        self._loaded_page = RecordFilePage(page_number)
        with open(self._filename, "rb+") as file:
            file.seek(self._page_size * page_number)
            bytes_read = 0
            while bytes_read < self._page_size:
                # It's the index
                index = int.from_bytes(file.read(Constants.INTEGER_SIZE), Constants.LITERAL)
                bytes_read += Constants.INTEGER_SIZE
                if index == maxsize:
                    return
                # It's the P(A)
                a_probability = binary_to_float(file.read(Constants.FLOAT_SIZE))
                # It's the P(B)
                b_probability = binary_to_float(file.read(Constants.FLOAT_SIZE))
                # It's the P(AUB)
                sum_probability = binary_to_float(file.read(Constants.FLOAT_SIZE))
                bytes_read += Constants.FLOAT_SIZE * 3
                self._loaded_page.add_last_record(Record(index, a_probability, b_probability, sum_probability))

    def save_page(self):
        with open(self._filename, "r+b") as file:
            file.seek(self._page_size * self._loaded_page.page_number)
            bytes_written = 0
            entry = self._loaded_page.remove_first_record()
            while bytes_written < self._page_size and entry is not None:
                file.write(entry.index.to_bytes(Constants.INTEGER_SIZE, Constants.LITERAL))
                bytes_written += Constants.INTEGER_SIZE
                file.write(float_to_binary(entry.a_probability))
                file.write(float_to_binary(entry.b_probability))
                file.write(float_to_binary(entry.sum_probability))
                bytes_written += Constants.FLOAT_SIZE * 3
                entry = self._loaded_page.remove_first_record()
            while bytes_written < self._page_size:
                file.write(sys.maxsize.to_bytes(Constants.FLOAT_SIZE, Constants.LITERAL))
                bytes_written += Constants.FLOAT_SIZE

    def get_record_by_index(self, index, page_number):
        if not self._loaded_page.page_number == page_number:
            self.save_page()
            self.load_existing_page(page_number)
        return self._loaded_page.get_record(index)

    def add_record(self, index, a_probability, b_probability, sum_probability):
        # Check if we can write it to the current page
        if self._loaded_page.get_number_of_records() < self._max_number_of_records:
            # Yes, there's empty space
            record = Record(index, a_probability, b_probability, sum_probability)
            self._loaded_page.add_last_record(record)
            return self._loaded_page.page_number
        else:
            # No, there's no space on this page. Check the last one.
            if self._loaded_page.page_number != self._number_of_pages - 1:
                # The currently loaded page is not the last one
                self.save_page()
                self.load_existing_page(self._number_of_pages - 1)
                return self.add_record(index, a_probability, b_probability, sum_probability)
            else:
                # Currently loaded page is the last one. There's no space in existing pages. Add a new one!
                self.save_page()
                self.create_new_page()
                record = Record(index, a_probability, b_probability, sum_probability)
                self._loaded_page.add_last_record(record)
                return self._loaded_page.page_number


class RecordFilePage:
    def __init__(self, page_number):
        self._records = []
        self._page_number = page_number

    def add_last_record(self, record):
        self._records.append(record)

    def remove_first_record(self):
        try:
            return self._records.pop(0)
        except IndexError:
            return None

    def get_number_of_records(self):
        return len(self._records)

    def get_record(self, index):
        return next((record for record in self._records if record.index == index), None)

    def create_new_page(self, new_page_number):
        self._page_number = new_page_number
        self._records.clear()

    @property
    def page_number(self):
        return self._page_number
