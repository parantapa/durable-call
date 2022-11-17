"""Robust utility functions."""

import asyncio

import structlog

logger = structlog.get_logger()


def cancel_all_tasks():
    loop = asyncio.get_running_loop()
    for task in asyncio.all_tasks(loop):
        logger.info("cancelling task", task_name=task.get_name())
        task.cancel()
