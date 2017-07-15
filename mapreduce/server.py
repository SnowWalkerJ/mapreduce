from multiprocess import Process, Queue
from time import sleep
from queue import Empty
from inspect import isgeneratorfunction
from functools import reduce


class MRServer(Process):
    def __init__(self, ith, queues, pipe, state, global_queue):
        self.ith = ith
        self.dataset = {}
        self.queues = queues
        self.queue = queues[ith]
        self.pipe = pipe
        self.state = state
        self.global_queue = global_queue
        self.__to_terminate = False
        super(MRServer, self).__init__()

    def run(self):
        while not self.__to_terminate:
            command = self.pipe.recv()
            self.state.value = 1
            func_name = command.pop('action')
            func = getattr(self, func_name)
            func(**command)
            self.state.value = 0

    def terminate(self):
        self.__to_terminate = True

    def add_dataset(self, name):
        if name not in self.dataset:
            self.dataset[name] = []
        dataset = self.dataset[name]
        retry = 0
        while 1:
            try:
                item = self.queue.get(True, timeout=0.1)
                dataset.append(item)
                retry = 0
            except Empty:
                retry += 1
            if retry >= 3:
                break

    def collect(self, name):
        for item in self.dataset[name]:
            self.global_queue.put(item)

    def remove_dataset(self, name):
        del self.dataset[name]

    def filter(self, src, dest, func):
        self.dataset[dest] = list(filter(func, self.dataset[src]))

    def map(self, src, dest, func):
        src = self.dataset[src]
        if isgeneratorfunction(func):
            self.dataset[dest] = []
            for item in src:
                self.dataset[dest].extend(list(func(item)))
        else:
            self.dataset[dest] = list(map(func, src))

    def partition(self, name):
        n = len(self.queues)
        dataset = self.dataset.pop(name)
        for item in dataset:
            key = chash(item[0]) % n
            self.queues[key].put(item)

    def reduce(self, src, dest, func):
        dataset = self.dataset[src]
        keys = set(item[0] for item in dataset)
        newdata = []
        for key in keys:
            group = (item[1] for item in dataset if item[0] == key)
            newdata.append((key, reduce(func, group)))
        self.dataset[dest] = newdata


    def count(self, name):
        self.pipe.send(len(self.dataset[name]))


def hash_string(s):
    if not s:
        return 0 # empty
    value = ord(s[0]) << 7
    for char in s:
        value = c_mul(1000003, value) ^ ord(char)
    value = value ^ len(s)
    if value == -1:
        value = -2
    return value


def c_mul(a, b):
    return int(hex((a * b) & 0xFFFFFFFF)[:-1], 16)


def chash(obj):
    if isinstance(obj, str):
        return hash_string(obj)
    else:
        return obj


