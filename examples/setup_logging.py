import sys
import logging

import structlog
from rich.console import Console
from rich.traceback import Traceback

def has_color() -> bool:
    return (
        sys.stdout is not None and hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
    )


def rich_traceback(sio, exc_info):
    sio.write("\n")
    if has_color():
        console = Console(file=sio, color_system="256")
    else:
        console = Console(file=sio, color_system="auto")
    console.print(Traceback.from_exception(*exc_info, show_locals=False))


def setup_logging():
    logging.basicConfig(
        format="%(asctime)s:%(message)s", stream=sys.stdout, level=logging.INFO
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S", utc=False),
            structlog.dev.ConsoleRenderer(
                colors=has_color(),
                exception_formatter=rich_traceback,
            ),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
