"""Wrapper to make fragile functions durable."""

import time
import asyncio as aio
from functools import wraps
from itertools import count
from datetime import datetime
from typing import Callable, Awaitable, Optional
from dataclasses import dataclass

import apsw
import structlog

from . import call_store as cstore

FragileFunction = Callable[[str, str], Awaitable[str]]
DurableFunction = Callable[[str, str], aio.Future[str]]
FragileToDurableWrapper = Callable[[FragileFunction], DurableFunction]

logger = structlog.get_logger()


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


class FragileFunctionExecutionError(Exception):
    def __init__(self, msg: str, call_id: str, func_name: str):
        self.msg = msg
        self.call_id = call_id
        self.func_name = func_name

    def __str__(self):
        return f"{self.msg}; call_id={self.call_id!r} func_name={self.func_name!r}"


class ResultLoggingFailedError(FragileFunctionExecutionError):
    def __init__(self, call_id: str, func_name: str):
        msg = "Failed to write result to log file"
        super().__init__(msg, call_id, func_name)


class CallTimeout(FragileFunctionExecutionError):
    def __init__(self, call_id: str, func_name: str):
        msg = "Call timeout"
        super().__init__(msg, call_id, func_name)


class CallFatalError(FragileFunctionExecutionError):
    def __init__(self, call_id: str, func_name: str):
        msg = "Call raised fatal error"
        super().__init__(msg, call_id, func_name)


class CallCancelledError(FragileFunctionExecutionError):
    def __init__(self, call_id: str, func_name: str):
        msg = "Call cancelled"
        super().__init__(msg, call_id, func_name)


class ParamsChangedError(ValueError):
    def __init__(self, call_id: str, cur_params: str, prev_params: str):
        self.call_id = call_id
        self.cur_params = cur_params
        self.prev_params = prev_params

    def __str__(self):
        return f"Call id {self.call_id!r} called with different params"


def _save_result(
    con: apsw.Connection,
    call_id: str,
    func_name: str,
    result: str,
    result_fut: aio.Future[str],
    log: structlog.BoundLogger,
) -> None:
    now = time.monotonic()
    try:
        with con:
            cstore.add_call_result(
                con,
                call_id=call_id,
                end_time=now,
                call_result=result,
            )
    except Exception:
        log.error("failed to save result", exc_info=True)
        exc = ResultLoggingFailedError(call_id, func_name)
        result_fut.set_exception(exc)
        return

    log.info("result saved")
    result_fut.set_result(result)
    return


async def _call_fragile_func(
    con: apsw.Connection,
    call_id: str,
    func_name: str,
    fragile_func: FragileFunction,
    call_params: str,
    retry_policy: RetryPolicy,
    result_fut: aio.Future[str],
    start_time: float,
    log: structlog.BoundLogger,
) -> None:
    now = time.monotonic()

    if now - start_time > retry_policy.max_retry_time:
        exc = CallTimeout(call_id, func_name)
        result_fut.set_exception(exc)
        return

    try:
        log.info("trying to call")
        result = await fragile_func(call_id, call_params)
        log.info("call succeeded")

        _save_result(
            con=con,
            call_id=call_id,
            func_name=func_name,
            result=result,
            result_fut=result_fut,
            log=log,
        )
        return

    except IntermittantError as e:
        log.info("call failed: intermittant error", error=str(e))
    except FatalError as e:
        log.error("call failed: fatal error", error=str(e), exc_info=True)
        exc = CallFatalError(call_id, func_name)
        result_fut.set_exception(exc)
        return
    except Exception as e:
        log.warning("call failed: unknown error", error=str(e), exc_info=True)


async def _task_robust_call_fragile_func(
    con: apsw.Connection,
    call_id: str,
    func_name: str,
    fragile_func: FragileFunction,
    call_params: str,
    retry_policy: RetryPolicy,
    result_fut: aio.Future[str],
) -> None:
    log = logger.bind(
        call_id=call_id,
        func_name=func_name,
    )

    start_time = time.monotonic()

    try:
        for try_count in count():
            log.bind(try_count=try_count)

            await _call_fragile_func(
                con=con,
                call_id=call_id,
                func_name=func_name,
                fragile_func=fragile_func,
                call_params=call_params,
                retry_policy=retry_policy,
                result_fut=result_fut,
                start_time=start_time,
                log=log,
            )
            if result_fut.done():
                return

            await aio.sleep(retry_policy.inter_retry_time)
    except aio.CancelledError as e:
        log.warning("call cancelled", error=str(e))
        exc = CallCancelledError(call_id, func_name)
        result_fut.set_exception(exc)
        return


class DurableFunctionExecutor:
    """A wrapper tracking durable functions."""

    def __init__(
        self,
    ):
        self.con: Optional[apsw.Connection] = None
        self.func_cache: dict[str, tuple[FragileFunction, RetryPolicy]] = {}
        self.pending_func_call: dict[str, tuple[aio.Task, aio.Future[str]]] = {}

    def initialize(self, con: apsw.Connection) -> None:
        self.con = con
        cstore.create_schema(con)
        self.load_unfinished_calls()

    def add_function(
        self, func_name: str, fragile_func: FragileFunction, retry_policy: RetryPolicy
    ) -> None:
        self.func_cache[func_name] = (fragile_func, retry_policy)

    def make_durable_call_fragile_func(
        self, call_id: str, func_name: str, call_params: str
    ) -> aio.Future[str]:
        assert self.con is not None

        loop = aio.get_running_loop()

        # Check if we know the function
        if func_name not in self.func_cache:
            raise RuntimeError("Call to unknown function: %r" % func_name)

        with self.con:
            # Check if we already have the result in the call log
            ret = cstore.get_call(self.con, call_id)
            if ret is not None:
                if call_params != ret.call_params:
                    raise ParamsChangedError(call_id, call_params, ret.call_params)

                if ret.call_result is not None:
                    fut = loop.create_future()
                    fut.set_result(ret.call_result)
                    return fut
                else:
                    # We have saved the parameters, but not the result.
                    # Dont log the params in the db again.

                    # Check if this call is already in progress
                    if call_id in self.pending_func_call:
                        _, fut = self.pending_func_call[call_id]
                        return fut

            else:
                # Log the params in the db
                now = datetime.utcnow().timestamp()
                cstore.add_call_params(
                    self.con,
                    call_id=call_id,
                    function_name=func_name,
                    start_time=now,
                    call_params=call_params,
                )

        fragile_func, retry_policy = self.func_cache[func_name]
        fut = loop.create_future()
        task = aio.create_task(
            _task_robust_call_fragile_func(
                con=self.con,
                call_id=call_id,
                func_name=func_name,
                fragile_func=fragile_func,
                call_params=call_params,
                retry_policy=retry_policy,
                result_fut=fut,
            ),
            name=f"robust_call_fragile_func:{call_id}",
        )
        self.pending_func_call[call_id] = (task, fut)

        return fut

    def load_unfinished_calls(self) -> None:
        assert self.con is not None

        loop = aio.get_running_loop()

        if self.pending_func_call:
            raise RuntimeError(
                "This method must be called before any new pending calls are registered"
            )

        with self.con:
            for uc in cstore.get_unfinished_calls(self.con):
                if uc.function_name not in self.func_cache:
                    raise RuntimeError(
                        "Call to unknown function: %r" % uc.function_name
                    )

                fragile_func, retry_policy = self.func_cache[uc.function_name]
                fut = loop.create_future()
                task = aio.create_task(
                    _task_robust_call_fragile_func(
                        con=self.con,
                        call_id=uc.call_id,
                        func_name=uc.function_name,
                        fragile_func=fragile_func,
                        call_params=uc.call_params,
                        retry_policy=retry_policy,
                        result_fut=fut,
                    ),
                    name=f"robust_call_fragile_func:{uc.call_id}",
                )
                self.pending_func_call[uc.call_id] = (task, fut)

    async def task_cleanup(self):
        try:
            while True:
                for call_id in list(self.pending_func_call):
                    task, _ = self.pending_func_call[call_id]
                    if task.done():
                        del self.pending_func_call[call_id]
                await aio.sleep(0)
        except aio.CancelledError:
            logger.info("task_cleanup cancelled")
            for call_id in list(self.pending_func_call):
                task, _ = self.pending_func_call[call_id]
                task.cancel()
                del self.pending_func_call[call_id]

    def durable_function(
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
                return self.make_durable_call_fragile_func(
                    call_id, func_name, call_params
                )

            return durable_func

        return wrapper
