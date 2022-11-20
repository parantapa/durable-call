"""Wrapper to make fragile functions durable."""

import asyncio as aio
from functools import wraps
from datetime import datetime
from typing import Callable, Awaitable, Optional

import apsw
import structlog
from structlog.contextvars import bound_contextvars

from . import call_store as cstore
from .robust import (
    FatalError,
    RobustCallTimeout,
)

FragileFunction = Callable[[str, bytes], Awaitable[bytes]]
DurableFunction = Callable[[str, bytes], aio.Future[bytes]]
FragileToDurableWrapper = Callable[[FragileFunction], DurableFunction]

logger = structlog.get_logger()


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
    def __init__(self, call_id: str, cur_params: bytes, prev_params: bytes):
        self.call_id = call_id
        self.cur_params = cur_params
        self.prev_params = prev_params

    def __str__(self):
        return f"Call id {self.call_id!r} called with different params"


async def _task_call_fragile_func(
    con: apsw.Connection,
    call_id: str,
    func_name: str,
    fragile_func: FragileFunction,
    call_params: bytes,
    result_fut: aio.Future[bytes],
) -> None:
    with bound_contextvars(call_id=call_id, func_name=func_name):
        try:
            try:
                logger.info("attempting call")
                result = await fragile_func(call_id, call_params)
                logger.info("call succeeded")
            except RobustCallTimeout:
                exc = CallTimeout(call_id, func_name)
                result_fut.set_exception(exc)
                return
            except FatalError:
                exc = CallFatalError(call_id, func_name)
                result_fut.set_exception(exc)
                return
            except Exception as e:
                result_fut.set_exception(e)
                return

            try:
                now = datetime.utcnow().timestamp()
                with con:
                    cstore.add_call_result(
                        con,
                        call_id=call_id,
                        end_time=now,
                        call_result=result,
                    )

                logger.info("result saved")
                result_fut.set_result(result)
            except Exception:
                logger.error("failed to save result", exc_info=True)
                exc = ResultLoggingFailedError(call_id, func_name)
                result_fut.set_exception(exc)

        except aio.CancelledError:
            logger.warning("call cancelled")
            exc = CallCancelledError(call_id, func_name)
            result_fut.set_exception(exc)


class DurableFunctionExecutor:
    """A wrapper tracking durable functions."""

    def __init__(
        self,
    ):
        self.con: Optional[apsw.Connection] = None
        self.func_cache: dict[str, FragileFunction] = {}
        self.pending_func_call: dict[str, tuple[aio.Task, aio.Future[bytes]]] = {}

    def initialize(self, con: apsw.Connection) -> None:
        self.con = con
        cstore.create_schema(con)
        self.load_unfinished_calls()

    def add_function(self, func_name: str, fragile_func: FragileFunction) -> None:
        self.func_cache[func_name] = fragile_func

    def make_durable_call_fragile_func(
        self, call_id: str, func_name: str, call_params: bytes
    ) -> aio.Future[bytes]:
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

        fragile_func = self.func_cache[func_name]
        fut = loop.create_future()
        task = aio.create_task(
            _task_call_fragile_func(
                con=self.con,
                call_id=call_id,
                func_name=func_name,
                fragile_func=fragile_func,
                call_params=call_params,
                result_fut=fut,
            ),
            name=f"call_fragile_func:{call_id}",
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

                fragile_func = self.func_cache[uc.function_name]
                fut = loop.create_future()
                task = aio.create_task(
                    _task_call_fragile_func(
                        con=self.con,
                        call_id=uc.call_id,
                        func_name=uc.function_name,
                        fragile_func=fragile_func,
                        call_params=uc.call_params,
                        result_fut=fut,
                    ),
                    name=f"call_fragile_func:{uc.call_id}",
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

    def make_durable(self, fragile_func: FragileFunction) -> DurableFunction:
        func_name = fragile_func.__qualname__

        self.add_function(
            func_name=func_name,
            fragile_func=fragile_func,
        )

        @wraps(fragile_func)
        def durable_func(call_id: str, call_params: bytes) -> aio.Future[bytes]:
            return self.make_durable_call_fragile_func(call_id, func_name, call_params)

        return durable_func
