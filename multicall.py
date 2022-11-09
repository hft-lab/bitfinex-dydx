from threading import Thread
import unittest


"""Call bunch of targets at the same time

Sample usage:
    import multicall
    my_pool = multicall.Pool()
    my_pool.add(some_method, some_arg, kw1=1, kw2=2)
    my_pool.add(other_method, other_args)
    my_pool.add(third_method)
    my_pool.call_and_wait() # will start all targets and wait until all done.

Please note: if you need to call:
    some_method(some_arg, kw1=1, kw2=2)
You have to use following syntax:
    my_pool.add(some_method, some_arg, kw1=1, kw2=2)

"""


class Pool:

    def __init__(self):
        self.calls = []

    def add(self, target, *args, **kwargs):
        self.calls.append([target, args, kwargs])

    def call_all_and_wait(self):
        threads = []
        for t in self.calls:
            thread = Thread(target=t[0], args=t[1], kwargs=t[2])
            thread.start()
            threads.append(thread)
        for t in threads:
            t.join()
        self.calls = []


class PoolTest(unittest.TestCase):

    def sample_func(self, seconds, kwarg1=1):
        import time
        from datetime import datetime
        print("\nsleeping", seconds, kwarg1, datetime.now())
        time.sleep(seconds)
        print("\nsleeping done", seconds, kwarg1, datetime.now())

    def test_pool(self):
        p = Pool()
        p.add(self.sample_func, 1, "test1")
        p.add(self.sample_func, 2, kwarg1="test2")
        p.call_all_and_wait()
        print("all done")
