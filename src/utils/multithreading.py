"""Multithreading functions"""


import threading
from queue import Queue
from src import logger


def parallel_execute(func, args, num_threads=1, **kwargs):
    """Runs the operation using multiple threads.
    
    Parameters
    ----------
    func : function
        The function to execute in multiple threads.
    
    args : list
        A list of values to use per function call.
        
    num_threads : int, default 1
        The number of threads in parallel to use.
        
    **kwargs
        Additional keyword arguments to pass to the provided function.
        
    Returns
    -------
    is_success : bool
        True if the execution succeeded.
    
    """

    caught_exceptions = []
    def run_func(queue):
        while True:
            arg = queue.get(block=True)
            try:
                func(arg, **kwargs)
            except Exception as e:
                caught_exceptions.append(e)
                
            queue.task_done()

    # Create queue and fill up with tasks
    queue = Queue()
    for a in args:
        logger.info('Adding to queue: {}'.format(a))
        queue.put(a)
        
    # Create threads
    for i in range(num_threads):
        logger.info('Creating thread: {}'.format(i+1))
        worker = threading.Thread(target=run_func, args=(queue,))
        worker.setDaemon(True)
        worker.start()
        
    # Wait until queue is empty
    queue.join()
    
    # Check if any exceptions occurred
    if len(caught_exceptions) > 0:
        logger.warning('\n{} errors occurred.'.format(len(caught_exceptions)))
        raise caught_exceptions[0]
        
    return True
