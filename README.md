# mapreduce

This is a python native module for standalone (single machine), in-memory, multiprocessing map-reduce operations.

## Usage

Below is a word counter using map-reduce
```python
from mapreduce.client import MRClient
from operator import add


if __name__ == '__main__':
    client = MRClient(4)              # Use 4 cores
    # or you can use: client = mapreduce.get_service() to get a global client
    s = "a fox is chasing after a rabbit chased by a fox".split(" ")
    print("The original sentence is", s)
    data = client.distribute('s', s)  # Put the data into multiple processes
    counter = data.map(lambda x: (x, 1), inplace=False) \
                  .reduce2(add)
    # reduce2 is equivalent to `.reduce().partition().reduce()`,
    # which is preferred to `.partition().reduce()`
    # for it reduce the data amount and therefore the time spent
    # on communicating between processes
    print(counter.collect())          # collect the result
```
```
The original sentence is ['a', 'fox', 'is', 'chasing', 'after', 'a', 'rabbit', 'chased', 'by', 'a', 'fox']
[('after', 1), ('a', 3), ('by', 1), ('is', 1), ('fox', 2), ('rabbit', 1), ('chasing', 1), ('chased', 1)]
```