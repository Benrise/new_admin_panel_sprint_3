
from functools import wraps
from time import sleep

from .logger import logger


def backoff(start_sleep_time=1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 1
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    error_msg = f'Backoff exception in function: {func.__name__}\n Next try in {int(min(t, border_sleep_time))} seconds\n'
                    logger.error(error_msg)
                    if (t < border_sleep_time):
                        t = start_sleep_time * (factor ** n)
                    n += 1
                    sleep(min(t, border_sleep_time))
        return inner
    return func_wrapper
