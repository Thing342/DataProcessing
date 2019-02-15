import threading
import time


class ElapsedTime:
    """To get a nicely-formatted elapsed time string for printing"""

    def __init__(self):
        self.start_time = time.time()

    def et(self):
        return "[{0:.1f}] ".format(time.time()-self.start_time)

class ErrorList:
    """Track a list of potentially fatal errors"""

    def __init__(self):
        self.lock = threading.Lock()
        self.error_list = []

    def add_error(self, e):
        self.lock.acquire()
        print("ERROR: " + e)
        self.error_list.append(e)
        self.lock.release()