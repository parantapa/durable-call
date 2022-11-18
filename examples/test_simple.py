"""Simple fragile function test."""

import asyncio
import random
from contextlib import closing

import apsw
import structlog

from durable_call import (
    DurableFunctionExecutor,
    CallFatalError,
    ParamsChangedError,
    IntermittantError,
    FatalError,
)
from durable_call.utils import cancel_all_tasks
from setup_logging import setup_logging

ONE_MIN = 60.0
ONE_SEC = 1.0

dce = DurableFunctionExecutor()
logger = structlog.get_logger()


def fragile_hello_world(who: str) -> str:
    if who == "hitler":
        raise ValueError("nope")

    if random.random() < 0.5:
        raise RuntimeError("bad things happen sometimes")

    if random.random() < 0.5:
        raise Exception("something unexpected")

    return "Hello %s" % who


@dce.durable_function(max_retry_time=ONE_MIN, inter_retry_time=ONE_SEC)
async def durable_hello_world(call_id: str, who: str) -> str:
    _ = call_id
    try:
        return fragile_hello_world(who)
    except RuntimeError as e:
        raise IntermittantError(e)
    except ValueError as e:
        raise FatalError(e)


async def hello_world_caller():
    logger.info("hello world caller started")
    try:
        result = await durable_hello_world("call1", "world")
        logger.info(result)

        try:
            result = await durable_hello_world("call1", "world1")
            logger.info(result)
        except ParamsChangedError as e:
            logger.info("got expected fatal error", error=e)
        else:
            logger.warning("didn't expected fatal error")

        try:
            result = await durable_hello_world("call2", "hitler")
            logger.info(result)
        except CallFatalError as e:
            logger.info("got expected fatal error", error=e)
        else:
            logger.warning("didn't expected fatal error")
    except asyncio.CancelledError:
        logger.info("hello world caller cancelled")
    except Exception as e:
        logger.error("hello world caller got unexpected exception: %s" % e)
    finally:
        cancel_all_tasks()


async def task_main():
    dbpath = "test_simple.db"
    con = apsw.Connection(dbpath)
    try:
        with closing(con):
            dce.initialize(con)

            task1 = asyncio.create_task(hello_world_caller(), name="hello_world_caller")
            task2 = asyncio.create_task(dce.task_cleanup(), name="dce_task_cleaner")

            await task1
            await task2
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    setup_logging()
    asyncio.run(task_main())
