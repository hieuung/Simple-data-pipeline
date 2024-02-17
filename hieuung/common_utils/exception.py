import logging
from functools import wraps

def debug(func):
    @wraps(func)
    def _execute(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(e)

    return _execute
