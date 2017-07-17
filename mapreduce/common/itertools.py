from inspect import isgenerator


def bufferize(iterable, buffer_size):
    def _bufferize_iterable(iterable, buffer_size):
        buffer = []
        for item in iterable:
            buffer.append(item)
            if len(buffer) >= buffer_size:
                yield buffer
                buffer = []
        if buffer:
            yield buffer
    
    def _bufferize_sequence(iterable, buffer_size):
        for i in range(0, len(iterable), buffer_size):
            batch = iterable[i:i+buffer_size]
            yield batch

    if isgenerator(iterable):
        return _bufferize_iterable(iterable, buffer_size)
    else:
        return _bufferize_sequence(iterable, buffer_size)
