from time import sleep
from collections import namedtuple
from contextlib import contextmanager
from queue import Empty
from multiprocess import Queue, Process, Pipe, Value
from .server import MRServer


Channel = namedtuple('Channel', ['queue', 'pipe', 'state'])


class StandardOperation:
    def __init__(self, action):
        self.action = action
        self.func = None

    def __get__(self, obj, type=None):
        if self.func is None:
            def function(func, dataset, inplace=True):
                if inplace:
                    name = dataset.name
                else:
                    postfix = hex(abs(id(func)))
                    name = "/".join([dataset.name, self.action, postfix])
                    obj.names.add(name)
                with obj.acquire():
                    obj._send({'action': self.action, 'src': dataset.name, 'dest': name, 'func': func})
                return Distributed(obj, name)
            function.__name__ = self.action
            self.func = function
        return self.func


class MRClient:
    map = StandardOperation("map")
    filter = StandardOperation("filter")
    reduce = StandardOperation("reduce")
    flatmap = StandardOperation("flatmap")
    def __init__(self, num_cores):
        self.num_cores = num_cores
        self.names = set()
        self.global_queue = Queue()
        self.pool = []
        self.channels = []
        self.__terminated = False
        queues = [Queue() for _ in range(num_cores)]
        for i in range(num_cores):
            pipe_master, pipe_slave = Pipe()
            state = Value('i', 0)
            process = MRServer(i, queues, pipe_slave, state, self.global_queue)
            self.channels.append(Channel(queues[i], pipe_master, state))
            self.pool.append(process)
            process.start()

    def wait(self, queue=True):
        while not self.idle(queue):
            sleep(0.01)

    def idle(self, queue=True):
        if queue:
            return self.global_queue.empty() and all(map(lambda x: x.state.value==0 and x.queue.empty(), self.channels))
        else:
            return all(map(lambda x: x.state.value==0, self.channels))
    
    @contextmanager
    def acquire(self, queue=True):
        self.wait(queue)
        yield
        self.wait(queue)
    
    def _send(self, item):
        for channel in self.channels:
            channel.pipe.send(item)

    def _recv(self):
        for channel in self.channels:
            yield channel.pipe.recv()

    def distribute(self, name, data):
        if name in self.names:
            raise KeyError("`%s` duplicated" % name)
        self.names.add(name)
        if isinstance(data, dict):
            data = data.items()
        with self.acquire():
            self._send({'action': "add_dataset", 'name': name})
            for i, item in enumerate(data):
                self.channels[i % self.num_cores].queue.put(item)
        return Distributed(self, name)

    # def map(self, func, dataset, inplace=True):
    #     if inplace:
    #         name = dataset.name
    #     else:
    #         postfix = hex(abs(id(func)))
    #         name = "/".join([dataset.name, "map", postfix])
    #         self.names.add(name)
    #     with self.acquire():
    #         self._send({'action': 'map', 'src': dataset.name, 'dest': name, 'func': func})
    #     return Distributed(self, name)

    # def reduce(self, func, dataset, inplace=True):
    #     if inplace:
    #         name = dataset.name
    #     else:
    #         postfix = hex(abs(id(func)))
    #         name = "/".join([dataset.name, "reduce", postfix])
    #         self.names.add(name)
    #     with self.acquire():
    #         self._send({'action': 'reduce', 'func': func, 'src': dataset.name, 'dest': name})
    #     return Distributed(self, name)

    # def filter(self, func, dataset, inplace=True):
    #     if inplace:
    #         name = dataset.name
    #     else:
    #         postfix = hex(abs(id(func)))
    #         name = "/".join([dataset.name, "filter", postfix])
    #         self.names.add(name)
    #     with self.acquire():
    #         self._send({'action': 'filter', 'src': dataset.name, 'dest': name, 'func': func})
    #     return Distributed(self, name)

    def partition(self, dataset):
        with self.acquire(queue=False):
            self._send({'action': 'partition', 'name': dataset.name})
        with self.acquire(queue=False):
            self._send({'action': "add_dataset", 'name': dataset.name})
        return dataset

    def merge(self, data):
        new_name = "/".join(["merge"] + [d.name for d in data])
        self._send({'action': 'merge',
                    'src': [d.name for d in data],
                    'dest': new_name})
        return Distributed(self, new_name)

    def count(self, dataset):
        n = 0
        with self.acquire():
            self._send({'action': 'count', 'name': dataset.name})
            for c in self._recv():
                n += c
        return n

    def remove(self, dataset):
        with self.acquire():
            self._send({'action': 'remove_dataset', 'name': dataset.name})
            self.names.remove(dataset.name)
    
    def exists(self, dataset):
        return dataset.name in self.names

    def terminate(self):
        with self.acquire():
            self._send({'action': 'terminate'})
        self.__terminated = True

    def collect(self, dataset):
        with self.acquire():
            self._send({'action': 'collect', 'name': dataset.name})
            data = []
            retry = 0
            while 1:
                try:
                    item = self.global_queue.get(True, timeout=0.1)
                    data.append(item)
                    retry = 0
                except Empty:
                    retry += 1
                if retry >= 3:
                    break
        return data

    def __del__(self):
        if not self.__terminated:
            self.terminate()


class Distributed:
    def __init__(self, client, name):
        self.name = name
        self.client = client

    def map(self, func, inplace=True):
        return self.client.map(func, self)

    def flatmap(self, func, inplace=True):
        return self.client.flatmap(func, self)

    def filter(self, func, inplace=True):
        return self.client.filter(func, self)

    def reduce(self, func, inplace=True):
        return self.client.reduce(func, self)

    def partition(self):
        return self.client.partition(self)

    def exists(self):
        return self.client.exists(self)

    def count(self):
        return self.client.count(self)

    def collect(self):
        return self.client.collect(self)

    def remove(self):
        return self.client.remove(self)

