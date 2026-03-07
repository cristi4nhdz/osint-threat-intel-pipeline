# config/decorator.py
"""Decorator to handle retry logic on fails."""

import time


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Retries a function with exponential backoff on failure."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            wait, attempts = delay, 0
            while attempts < max_attempts - 1:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    attempts += 1
                    time.sleep(wait)
                    wait *= backoff
            return func(*args, **kwargs)

        return wrapper

    return decorator
