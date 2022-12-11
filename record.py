class Record:
    def __init__(self, index, a_probability, b_probability, sum_probability):
        self._index = index
        self._a_probability = a_probability
        self._b_probability = b_probability
        self._sum_probability = sum_probability

    def serialize(self):
        pass

    def __gt__(self, other):
        return self._index > other.index

    def __ge__(self, other):
        return self._index >= other.index

    def __lt__(self, other):
        return self._index < other.index

    def __le__(self, other):
        return self._index <= other.index

    @property
    def index(self):
        return self._index

    @property
    def a_probability(self):
        return self._a_probability

    @property
    def b_probability(self):
        return self._b_probability

    @property
    def sum_probability(self):
        return self._sum_probability
