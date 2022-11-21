"""Simple fragile function using subprocess test."""

import shlex
import asyncio
from contextlib import closing

import apsw
import structlog

from durable_call import (
    FatalError,
    IntermittantError,
    DurableFunctionExecutor,
    CallFailed,
    make_robust,
)
from durable_call.utils import cancel_all_tasks
from setup_logging import setup_logging

dce = DurableFunctionExecutor()
logger = structlog.get_logger()


async def fragile_sleep_and_get_even_number(sleep_time: int) -> str:
    cmd = f"/bin/bash -c 'sleep {sleep_time} ; echo $RANDOM'"
    cmd = shlex.split(cmd)

    try:
        proc = await asyncio.subprocess.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE
        )
    except OSError as e:
        raise FatalError(f"Unexpected error: %s" % e)
    except ValueError as e:
        raise FatalError(f"Unexpected error: %s" % e)

    try:
        sout, _ = await asyncio.wait_for(proc.communicate(), timeout=1.0)
        sout = sout.decode()
        sout = int(sout)
        if sout % 2:
            return f"we got an even number: {sout}"
        else:
            raise IntermittantError("we got an odd number")
    except asyncio.TimeoutError:
        raise IntermittantError("call timed out")
    finally:
        if proc.returncode is None:
            proc.kill()


@dce.make_durable
@make_robust(max_retry_time=10, inter_retry_time=1)
async def durable_sleep_and_get_even_number(call_id: str, params: bytes) -> bytes:
    _ = call_id
    try:
        sleep_time = int(params.decode())
    except ValueError:
        raise FatalError("Invalid params: %r" % params)

    ret = await fragile_sleep_and_get_even_number(sleep_time)
    return ret.encode()


async def shell_func_caller():
    logger.info("shell func caller started")
    try:
        result = await durable_sleep_and_get_even_number("call1", "0".encode())
        logger.info(result)

        try:
            result = await durable_sleep_and_get_even_number("call2", "1".encode())
            logger.info(result)
        except CallFailed as e:
            logger.info("got expected error", error=str(e))
        else:
            logger.warning("didn't expected fatal error")

        cancel_all_tasks()
    except asyncio.CancelledError:
        logger.info("shell func caller cancelled")
    except Exception as e:
        logger.error("shell func caller got unexpected exception: %s" % e)
        cancel_all_tasks()


async def task_main():
    dbpath = "test_subprocess.db"
    con = apsw.Connection(dbpath)
    try:
        with closing(con):
            dce.initialize(con)

            task1 = asyncio.create_task(shell_func_caller(), name="shell_func_caller")
            task2 = asyncio.create_task(dce.task_cleanup(), name="dce_task_cleaner")

            await task1
            await task2
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    setup_logging()
    asyncio.run(task_main())
