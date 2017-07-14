# mapreduce

This is a python native module for standalone (single machine), in-memory, multiprocessing map-reduce operations.

## Usage

Below is a word counter using map-reduce
```python
from mapreduce.client import MRClient
from operator import add

def func(x):          # You need a function instead of lambda because the backend is multiprocessing
    return (x, 1)


if __name__ == '__main__':
    client = MRClient(4)              # Use 4 cores
    s = "a fox is chasing after a rabbit chased by a fox".split(" ")
    print("The original sentence is", s)
    data = client.distribute('s', s)  # Put the data into multiple processes
    data = data.map(func)
    data.partition()
    data = data.reduce(add)
    print(data.collect())            # collect the result
```
```
The original sentence is ['a', 'fox', 'is', 'chasing', 'after', 'a', 'rabbit', 'chased', 'by', 'a', 'fox']
[('after', 1), ('a', 3), ('by', 1), ('is', 1), ('fox', 2), ('rabbit', 1), ('chasing', 1), ('chased', 1)]
```