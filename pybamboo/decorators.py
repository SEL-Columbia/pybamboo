import math
import time

from pybamboo.exceptions import PyBambooException


RETRY_DELAY = 3
RETRY_BACKOFF = 1.5


def require_valid(func):
    """
    This decorator checks whether or not this object corresponds to
    a valid dataset in bamboo.  If not, it raises an exception.
    """
    def wrapped(self, *args, **kwargs):
        if self._id is None:
            raise PyBambooException('Dataset does not exist.')
        return func(self, *args, **kwargs)
    return wrapped


def retry(tries, delay=RETRY_DELAY, backoff=RETRY_BACKOFF):
    '''
    Adapted from code found here:
        http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    Retries a function or method until it returns True.

    *delay* sets the initial delay in seconds, and *backoff* sets the
    factor by which the delay should lengthen after each failure.
    *backoff* must be greater than 1, or else it isn't really a backoff.
    *tries* must be at least 0, and *delay* greater than 0.
    '''

    if backoff <= 1:  # pragma: no cover
        raise ValueError("backoff must be greater than 1")

    tries = math.floor(tries)
    if tries < 0:  # pragma: no cover
        raise ValueError("tries must be 0 or greater")

    if delay <= 0:  # pragma: no cover
        raise ValueError("delay must be greater than 0")

    def decorator_retry(func):
        def function_retry(self, *args, **kwargs):
            mtries, mdelay = tries, delay
            result = func(self, *args, **kwargs)
            while mtries > 0:
                if result:
                    return result
                mtries -= 1
                time.sleep(mdelay)
                mdelay *= backoff
                result = func(self, *args, **kwargs)
            return False

        return function_retry
    return decorator_retry
