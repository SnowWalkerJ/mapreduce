from time import sleep
from collections import namedtuple
from contextlib import contextmanager
from queue import Empty
from multiprocess import Queue, Process, Pipe, Value
from .server import MRServer
from .common.settings import CONFIG
from .common.itertools import bufferize
from .common.io import robust_recv


Channel = namedtuple('Channel', ['queue', 'pipe', 'state'])


class StandardOperation:
    def __init__(self, action):
        self.action = action
        self.funcs = {}

    def __get__(self, obj, type=None):
        if id(obj) not in self.funcs:
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
            self.funcs[id(obj)] = function
        return self.funcs[id(obj)]


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
        """
        Wait until the processes are idle
        """
        self.wait(queue)
        yield
    
    def _send(self, item):
        """Send commands to processes"""
        for channel in self.channels:
            channel.pipe.send(item)

    def _recv(self):
        """
        Receive info from processes.
        """
        for channel in self.channels:
            yield channel.pipe.recv()

    def distribute(self, name, data):
        """
        Distribute the data to processes. 
        
        Parameters
        ----------
        name: str
            Store the data by a key `name`.
        data: Iterable
            the data to be distributed.
        """
        if name in self.names:
            raise KeyError("`%s` duplicated" % name)
        self.names.add(name)
        if isinstance(data, dict):
            data = data.items()
        i = 0
        n = self.num_cores
        with self.acquire():
            self._send({'action': "add_dataset", 'name': name})
            for batch in bufferize(data, CONFIG.BUFFER_SIZE):
                self.channels[i % n].queue.put(batch)
                i += 1
        return Distributed(self, name)

    def reduce2(self, dataset, func, inplace=True):
        """
        Step 1: Reduce in seperate processes;
        Step 2: Partition;
        Step 3: Reduce in seperate processes again.
        """
        data = dataset.reduce(func, inplace=inplace) \
                      .partition() \
                      .reduce(func,inplace=True)
        return data

    def partition(self, dataset, by=None):
        """
        Redistribute objects to different processes according to
        their key.

        Parameters
        ----------
        by: object -> Union[str, num]
            a function that maps object to a key,
            this key decides which process to put
            the data on.
        """
        with self.acquire():
            self._send({'action': 'partition', 'name': dataset.name, 'by': by})
        with self.acquire(queue=False):
            self._send({'action': "add_dataset", 'name': dataset.name})
        return dataset

    def merge(self, data):
        """
        Merges multiple datasets to a new one.
        """
        new_name = "/".join(["merge"] + [d.name for d in data])
        self._send({'action': 'merge',
                    'src': [d.name for d in data],
                    'dest': new_name})
        return Distributed(self, new_name)

    def count(self, dataset):
        """
        Returns
        -------
        The length of the dataset
        """
        n = 0
        with self.acquire():
            self._send({'action': 'count', 'name': dataset.name})
            for c in self._recv():
                n += c
        return n

    def remove(self, dataset):
        """
        Remove the data from processes.
        """
        with self.acquire():
            self._send({'action': 'remove_dataset', 'name': dataset.name})
            self.names.remove(dataset.name)
    
    def exists(self, dataset):
        """
        Returns
        -------
        Whether the data exists on the processes, or
        it it removed already.
        """
        return dataset.name in self.names

    def terminate(self):
        """
        Terminate the processes.
        """
        with self.acquire():
            self._send({'action': 'terminate'})
        self.__terminated = True

    def collect(self, dataset):
        """
        Collect the data from processes to the client.
        """
        with self.acquire():
            self._send({'action': 'collect', 'name': dataset.name})
            data = robust_recv(self.global_queue, batch=True, retries=3)
        return data

    def __del__(self):
        if not self.__terminated:
            self.terminate()


class Distributed:
    def __init__(self, client, name):
        self.name = name
        self.client = client

    def map(self, func, inplace=True):
        return self.client.map(func, self, inplace=inplace)

    def flatmap(self, func, inplace=True):
        return self.client.flatmap(func, self, inplace=inplace)

    def filter(self, func, inplace=True):
        return self.client.filter(func, self, inplace=inplace)

    def reduce(self, func, inplace=True):
        return self.client.reduce(func, self, inplace=inplace)

    def partition(self), by=None:
        return self.client.partition(self, by=by)

    def exists(self):
        return self.client.exists(self)

    def count(self):
        return self.client.count(self)

    def collect(self):
        return self.client.collect(self)

    def remove(self):
        return self.client.remove(self)

