"""Wrapper to retry functions that can fail intermittantly."""

from collections.abc import Awaitable
import time
import asyncio
from functools import wraps
from itertools import count
from dataclasses import dataclass
from typing import Callable, ParamSpec, TypeVar

import structlog
from structlog.contextvars import bound_contextvars


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


logger = structlog.get_logger()

P = ParamSpec("P")
R = TypeVar("R")


def make_robust(
    *args, **kwargs
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    retry_policy = RetryPolicy(*args, **kwargs)

    def decorator(wrapped: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(wrapped)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start_time = time.monotonic()
            for try_count in count(start=1):
                with bound_contextvars(try_count=try_count):
                    try:
                        return await wrapped(*args, **kwargs)
                    except IntermittantError as e:
                        logger.info(
                            "call failed: intermittant error",
                            error=str(e),
                        )
                    except FatalError as e:
                        logger.error(
                            "call failed: fatal error",
                            error=str(e),
                        )
                        raise
                    except Exception as e:
                        logger.warning(
                            "call failed: unknown error",
                            error=str(e),
                            exc_info=True,
                        )

                await asyncio.sleep(retry_policy.inter_retry_time)

                now = time.monotonic()
                elapsed = now - start_time
                if elapsed > retry_policy.max_retry_time:
                    logger.error(
                        "call failed: timeout",
                        elapsed=elapsed,
                        max_retry_time=retry_policy.max_retry_time,
                    )
                    raise RobustCallTimeout("Call timed out")

            raise RuntimeError("should never reach here")

        return wrapper

    return decorator
