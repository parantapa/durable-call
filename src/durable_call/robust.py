"""Wrapper to retry functions that can fail intermittantly."""

import time
import asyncio
from functools import wraps
from itertools import count
from dataclasses import dataclass


@dataclass
class RetryPolicy:
    max_retry_time: float
    inter_retry_time: float


class IntermittantError(Exception):
    """A known error that occurs intermittently.

    The durable function executor will log the error message
    and continue retrying.
    """


class FatalError(Exception):
    """A known error that should not have happened.

    The durable function executor will log the error message
    along with the traceback.
    It will also stop executing any further, raise a RuntimeError and exit.
    """


class RobustCallTimeout(TimeoutError):
    pass


def make_robust(*args, **kwargs):
    retry_policy = RetryPolicy(*args, **kwargs)

    def decorator(wrapped):
        @wraps(wrapped)
        async def wrapper(*args, log, **kwargs):
            start_time = time.monotonic()
            for try_count in count(start=1):
                log.bind(try_count=try_count)

                try:
                    return await wrapped(*args, **kwargs)
                except IntermittantError as e:
                    log.info(
                        "call failed: intermittant error",
                        error=str(e),
                        try_count=try_count,
                    )
                except FatalError as e:
                    log.error(
                        "call failed: fatal error",
                        error=str(e),
                    )
                    raise
                except Exception as e:
                    log.warning(
                        "call failed: unknown error",
                        error=str(e),
                        exc_info=True,
                    )

                await asyncio.sleep(retry_policy.inter_retry_time)

                now = time.monotonic()
                elapsed = now - start_time
                if elapsed > retry_policy.max_retry_time:
                    log.error(
                        "call failed: timeout",
                        elapsed=elapsed,
                        max_retry_time=retry_policy.max_retry_time,
                    )
                    raise RobustCallTimeout("Call timed out")

        return wrapper

    return decorator
