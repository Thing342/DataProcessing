import threading
import time


class ElapsedTime:
    """To get a nicely-formatted elapsed time string for printing"""

    def __init__(self):
        self.start_time = time.time()

    def et(self):
        return "[{0:.1f}] ".format(time.time() - self.start_time)


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


def format_clinched_mi(clinched, total):
    """return a nicely-formatted string for a given number of miles
    clinched and total miles, including percentage"""
    percentage = "-.-%"
    if total != 0.0:
        percentage = "({0:.1f}%)".format(100 * clinched / total)
    return "{0:.2f}".format(clinched) + " of {0:.2f}".format(total) + \
           " mi " + percentage
