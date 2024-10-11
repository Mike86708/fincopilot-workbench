import time
from contextlib import contextmanager

@contextmanager
def timer(section_name):
    '''Context manager to time sections of code'''
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time
    #print(f"{section_name} took {elapsed_time:.4f} seconds.")
