from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import threading
import time
import unittest

"""
Based on

Logical Physical Clocks and Consistent Snapshots in
Globally Distributed Databases

https://www.cse.buffalo.edu/tech-reports/2014-04.pdf
"""


def synchronized(fn):
    """Synchronization for object methods using implicit lock, self.lock"""
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return fn(self, *args, **kwargs)
    return wrapper


class HLC:
    usec_per_sec = 1e6
    kLogicalBits = 12
    kLogicalMask = (1 << kLogicalBits) - 1

    @staticmethod
    def fromNow():
        hlc = HLC()
        hlc.fromtime(time.time())
        return hlc

    def __init__(self, micros=0, logical=0):
        self.format = "%s.%f"
        self.lock = threading.Lock()
        self._set(micros, logical)

    def fromtime(self, t):
        "Takes time in seconds in epoch as a float"
        now = int(t * self.usec_per_sec)
        self._set(now, 0)

    def fromhlc(self, hlc):
        self.hlc = hlc

    def _set(self, micros, logical):
        self.hlc = (micros << self.kLogicalBits) | logical

    def set_format(self, format):
        "Used by __str__ method"
        self.format = format

    def tuple(self):
        """Returns a tuple of <microseconds since epoch, logical clock>"""
        return self.hlc >> self.kLogicalBits, self.hlc & self.kLogicalMask

    def __str__(self):
        format = self.format
        physical_time, logical_time = self.tuple()
        physical_time = physical_time / self.usec_per_sec
        physical_time = datetime.datetime.fromtimestamp(physical_time)
        return '{} {}'.format(physical_time.strftime(format), logical_time)

    def __eq__(self, other):
        return self.tuple() == other.tuple()

    def __lt__(self, other):
        return self.tuple() < other.tuple()

    def __sub__(self, other):
        "Ignores logical. Difference in microsecs"
        micros1, _ = self.tuple()
        micros2, _ = other.tuple()
        return micros1 - micros2

    # The next two methods are almost reproduced verbatim from the paper
    @synchronized
    def get(self):
        "To be used on send"
        wall = HLC.fromNow()
        micros, logical = self.tuple()
        wmicros, wlogical = wall.tuple()
        original_micros = micros
        micros = max(micros, wmicros)
        if micros == original_micros:
            logical = logical + 1
        else:
            logical = 0
        self._set(micros, logical)

    @synchronized
    def update(self, event):
        "To be used on receive"
        micros, logical = self.tuple()
        original_micros = micros
        emicros, elogical = event.tuple()
        wall = HLC.fromNow()
        wmicros, wlogical = wall.tuple()
        micros = max(micros, emicros, wmicros)
        if micros == emicros and micros == original_micros:
            logical = max(logical, elogical) + 1
        elif micros == original_micros:
            logical = logical + 1
        elif micros == emicros:
            logical = elogical + 1
        else:
            logical = 0
        self._set(micros, logical)


class HLCTests(unittest.TestCase):

    def test_comparison(self):
        h1 = HLC()
        h2 = HLC.fromNow()
        # set h1 to be 1000 seconds in the future
        h1.fromtime(time.time() + 1000)
        self.assertEqual(h2, h2)
        self.assertEqual(h2 < h1, True)

    def test_send(self):
        h1 = HLC()
        # set h1 to be 1000 seconds in the future
        h1.fromtime(time.time() + 1000)
        h1.get()
        micros, logical = h1.tuple()
        # Logical must have ticked, because micros
        # should be in the future
        self.assertEqual(logical, 1)

    def test_receive(self):
        h1 = HLC()
        # set h1 to be 3 seconds in the future
        h1.fromtime(time.time() + 3)
        original_micros, _ = h1.tuple()
        event = HLC()
        # event is 2 seconds in the future
        event.fromtime(time.time() + 2)
        h1.update(event)
        micros, logical = h1.tuple()
        self.assertEqual(logical, 1)
        self.assertAlmostEqual(micros, original_micros)
        time.sleep(3)
        # The wall clock catches up to HLC and resets logical
        h1.get()
        micros, logical = h1.tuple()
        self.assertEqual(logical, 0)

if __name__ == '__main__':
    unittest.main()
