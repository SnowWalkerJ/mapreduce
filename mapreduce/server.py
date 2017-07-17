from multiprocess import Process, Queue
from time import sleep
from queue import Empty
from inspect import isgeneratorfunction
from functools import reduce
from operator import itemgetter
from .common.settings import CONFIG
from .common.itertools import bufferize
from .common.io import robust_recv


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
        self.dataset[name].extend(robust_recv(self.queue))

    def collect(self, name):
        for batch in bufferize(self.dataset[name], CONFIG.BUFFER_SIZE):
            self.global_queue.put(batch)

    def remove_dataset(self, name):
        del self.dataset[name]

    def filter(self, src, dest, func):
        self.dataset[dest] = list(filter(func, self.dataset[src]))

    def flatmap(self, src, dest, func):
        src = self.dataset[src]
        self.dataset[dest] = []
        for item in src:
            self.dataset[dest].extend(list(func(item)))

    def map(self, src, dest, func):
        self.dataset[dest] = list(map(func, self.dataset[src]))

    def partition(self, name, by=None):
        by = by or itemgetter(0)
        n = len(self.queues)
        dataset = self.dataset.pop(name)
        buffers = [[] for _ in range(n)]
        for item in dataset:
            key = chash(by(item)) % n
            buffers[key].append(item)
            if len(buffers[key]) >= CONFIG.BUFFER_SIZE:
                self.queues[key].put(buffers[key])
                buffers[key] = []
        for i in range(n):
            if buffers[i]:
                self.queues[i].put(buffers[i])

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

    def merge(self, src, dest):
        dataset = []
        for s in src:
            dataset.extend(self.dataset[s])
        self.dataset[dest] = dataset


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


