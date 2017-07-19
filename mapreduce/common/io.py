from queue import Empty


def robust_recv(queue, retries=3):
    """
    Try to receive from a queue with retries

    Parameters
    ----------
    queue: Queue
        the queue to receive from
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
            yield item
            retry = 0
        except Empty:
            retry += 1
            if retry >= 3:
                break
    return data
