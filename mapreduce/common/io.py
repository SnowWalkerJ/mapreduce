from queue import Empty


def robust_recv(queue, batch=True, retries=3):
    """
    Try to receive from a queue with retries

    Parameters
    ----------
    queue: Queue
        the queue to receive from
    batch: bool, optional
        if True, assuming the received item is a list containing
        batch of objects
    retries: int
        times of retry
    
    Returns
    -------
    list
        total data received
    """
    data = []
    retry = 0
    while 1:
        try:
            item = queue.get(True, timeout=0.01)
            if batch:
                data.extend(item)
            else:
                data.append(item)
            retry = 0
        except Empty:
            retry += 1
            if retry >= 3:
                break
    return data
