"""Wrapper to make fragile functions durable."""

import asyncio as aio
from functools import wraps
from datetime import datetime
from typing import Callable, Optional
from dataclasses import dataclass

import apsw
import structlog

from . import call_store as cstore

FragileFunction = Callable[[str, str], str]
DurableFunction = Callable[[str, str], aio.Future[str]]
FragileToDurableWrapper = Callable[[FragileFunction], DurableFunction]


def cancel_all_tasks():
    loop = aio.get_running_loop()
    for task in aio.all_tasks(loop):
        task.cancel()


@dataclass
class PendingCall:
    function_name: str
    call_params: str
    start_time: float
    result_futures: list[aio.Future]

    try_count: int
    next_try_after: float


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


class PendingCallExecutionError(Exception):
    def __init__(self, msg: str, call_id: str):
        self.msg = msg
        self.call_id = call_id

    def __str__(self):
        return f"{self.msg}; call_id={self.call_id!r}"


class DurableFunctionExecutor:
    """A wrapper tracking durable functions."""

    def __init__(
        self,
    ):
        self.con: Optional[apsw.Connection] = None
        self.func_cache: dict[str, tuple[FragileFunction, RetryPolicy]] = {}
        self.pending_calls: dict[str, PendingCall] = {}

    def initialize(self, con: apsw.Connection) -> None:
        self.con = con
        cstore.create_schema(con)
        self.load_unfinished_calls()

    def add_function(
        self, func_name: str, fragile_func: FragileFunction, retry_policy: RetryPolicy
    ) -> None:
        self.func_cache[func_name] = (fragile_func, retry_policy)

    def make_durable_call(
        self, call_id: str, func_name: str, call_params: str
    ) -> aio.Future[str]:
        assert self.con is not None

        loop = aio.get_running_loop()
        fut = loop.create_future()
        now = datetime.utcnow().timestamp()

        # Check if we know the function
        if func_name not in self.func_cache:
            raise RuntimeError("Call to unknown function: %r" % func_name)

        # Check if this call is already in progress
        if call_id in self.pending_calls:
            self.pending_calls[call_id].result_futures.append(fut)
            return fut

        with self.con:
            # Check if this call has already completed
            ret = cstore.get_call(self.con, call_id)
            if ret is not None:
                fut.set_result(ret.call_result)
                return fut

            # Create a pending call
            cstore.add_call_params(
                self.con,
                call_id=call_id,
                function_name=func_name,
                start_time=now,
                call_params=call_params,
            )

            self.pending_calls[call_id] = PendingCall(
                function_name=func_name,
                call_params=call_params,
                start_time=now,
                result_futures=[fut],
                try_count=0,
                next_try_after=now,
            )

        return fut

    def load_unfinished_calls(self) -> None:
        assert self.con is not None

        if self.pending_calls:
            raise RuntimeError(
                "This method must be called before any new pending calls are registered"
            )

        now = datetime.utcnow().timestamp()

        with self.con:
            for uc in cstore.get_unfinished_calls(self.con):
                if uc.function_name not in self.func_cache:
                    raise RuntimeError(
                        "Call to unknown function: %r" % uc.function_name
                    )

                self.pending_calls[uc.call_id] = PendingCall(
                    function_name=uc.function_name,
                    call_params=uc.call_params,
                    start_time=now,
                    result_futures=[],
                    try_count=0,
                    next_try_after=now,
                )

    def do_execute_pending_call(self, call_id: str) -> None:
        assert self.con is not None

        pending_call = self.pending_calls[call_id]
        fragile_func, retry_policy = self.func_cache[pending_call.function_name]
        logger = structlog.get_logger(
            call_id=call_id,
            function_name=pending_call.function_name,
            try_count=pending_call.try_count,
        )

        now = datetime.utcnow().timestamp()
        if pending_call.next_try_after > now:
            return

        try:
            logger.info("trying to call")
            result = fragile_func(call_id, pending_call.call_params)
            logger.info("call succeeded")

            now = datetime.utcnow().timestamp()
            try:
                with self.con:
                    cstore.add_call_result(
                        self.con,
                        call_id=call_id,
                        end_time=now,
                        call_result=result,
                    )
            except Exception:
                logger.error("failed to save result", exc_info=True)
                raise PendingCallExecutionError(
                    "Failed to write result to log file", call_id
                )

            logger.info("result saved")

            for fut in pending_call.result_futures:
                fut.set_result(result)

            del self.pending_calls[call_id]
        except IntermittantError as e:
            logger.info("call failed: intermittant error", error=str(e))
            now = datetime.utcnow().timestamp()
            pending_call.next_try_after = now + retry_policy.inter_retry_time
            next_elapsed = pending_call.next_try_after - pending_call.start_time
            if next_elapsed > retry_policy.max_retry_time:
                raise PendingCallExecutionError("Call timeout", call_id)
            pending_call.try_count += 1
        except FatalError as e:
            logger.error("call failed: fatal error", error=str(e), exc_info=True)
            raise PendingCallExecutionError("Call failed due to fatal error", call_id)
        except Exception as e:
            logger.warning("call failed: unknown error", error=str(e), exc_info=True)
            now = datetime.utcnow().timestamp()
            pending_call.next_try_after = now + retry_policy.inter_retry_time
            next_elapsed = pending_call.next_try_after - pending_call.start_time
            if next_elapsed > retry_policy.max_retry_time:
                raise PendingCallExecutionError("Call timeout", call_id)
            pending_call.try_count += 1

    async def execute_pending_calls(self) -> None:
        try:
            while True:
                logger = structlog.get_logger()
                # logger.debug("executing pending calls", num_pending_calls=len(self.pending_calls))
                for call_id in list(self.pending_calls):
                    self.do_execute_pending_call(call_id)

                await aio.sleep(0)
        except aio.CancelledError as e:
            logger = structlog.get_logger()
            logger.info("pending call executor cancelled")
            cancel_all_tasks()
            return
        except PendingCallExecutionError as e:
            logger = structlog.get_logger()
            logger.warn("pending call executor exited due to error", error=str(e))
            cancel_all_tasks()
            return
        except Exception as e:
            logger = structlog.get_logger()
            logger.error(
                "unexpected expection stopped the pending call executor", exc_info=True
            )
            cancel_all_tasks()
            return

    def durable(
        self, max_retry_time: float, inter_retry_time: float
    ) -> FragileToDurableWrapper:
        def wrapper(fragile_func: FragileFunction) -> DurableFunction:
            func_name = fragile_func.__qualname__

            self.add_function(
                func_name=func_name,
                fragile_func=fragile_func,
                retry_policy=RetryPolicy(
                    max_retry_time=max_retry_time, inter_retry_time=inter_retry_time
                ),
            )

            @wraps(fragile_func)
            def durable_func(call_id: str, call_params: str) -> aio.Future[str]:
                return self.make_durable_call(call_id, func_name, call_params)

            return durable_func

        return wrapper
