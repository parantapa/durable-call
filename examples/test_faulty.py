"""Sample faulty program."""

import asyncio
import random
from contextlib import closing

import apsw

from durable_call.durable import DurableFunctionExecutor, IntermittantError, FatalError
from setup_logging import setup_logging

ONE_MIN = 60.0
ONE_SEC = 1.0

dce = DurableFunctionExecutor()


def fragile_hello_world(who: str) -> str:
    if who == "hitler":
        raise ValueError("nope")

    if random.random() < 0.5:
        raise RuntimeError("bad things happen sometimes")

    if random.random() < 0.5:
        raise Exception("something unexpected")

    return "Hello %s" % who


@dce.durable(max_retry_time=ONE_MIN, inter_retry_time=ONE_SEC)
def durable_hello_world(call_id: str, who: str) -> str:
    _ = call_id
    try:
        return fragile_hello_world(who)
    except RuntimeError as e:
        raise IntermittantError(e)
    except ValueError as e:
        raise FatalError(e)


async def hello_world_caller():
    print("hello world caller started")
    try:
        fut = durable_hello_world("call1", "world")
        result = await fut
        print(result)

        fut = durable_hello_world("call2", "hitler")
        result = await fut
        print(result)
    except asyncio.CancelledError:
        print("hello world caller cancelled")


async def main():
    dbpath = "hello.db"
    con = apsw.Connection(dbpath)
    with closing(con):
        dce.initialize(con)

        task1 = asyncio.create_task(hello_world_caller())
        task2 = asyncio.create_task(dce.execute_pending_calls())

        await task1
        await task2


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())
