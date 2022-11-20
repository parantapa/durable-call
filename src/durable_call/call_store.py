"""call_store

This module has been generated with SqlPyGen from call_store.sql.
"""

from dataclasses import dataclass
from typing import Optional, Iterable

import apsw

ConnectionType = apsw.Connection

SCHEMA = {}
SCHEMA[
    "call_log"
] = """
create table if not exists call_log (
    call_id text primary key,
    function_name text not null,
    start_time real not null,
    call_params blob not null,

    end_time real,
    call_result blob
)
"""

SCHEMA[
    "call_log_index1"
] = """
create index if not exists call_log_index1
on call_log (call_result)
where call_result is null
"""


QUERY = {}
QUERY[
    "add_call_params"
] = """
insert into call_log values (
    :call_id, :function_name, :start_time, :call_params, null, null
)
"""

QUERY[
    "add_call_result"
] = """
update call_log
set end_time = :end_time, call_result = :call_result
where call_id = :call_id
"""

QUERY[
    "get_unfinished_calls"
] = """
select call_id, function_name, call_params
from call_log
where call_result is null
order by start_time asc
"""

QUERY[
    "get_call"
] = """
select function_name, call_params, call_result
from call_log
where call_id = :call_id
"""


@dataclass
class GetUnfinishedCallsReturnType:
    call_id: str
    function_name: str
    call_params: bytes


@dataclass
class GetCallReturnType:
    function_name: str
    call_params: bytes
    call_result: Optional[bytes]


def create_schema(connection: ConnectionType) -> None:
    """Create the table schema."""
    with connection:
        cursor = connection.cursor()

        try:
            sql = SCHEMA["call_log"]

            cursor.execute(sql)
        except Exception as e:
            raise RuntimeError(
                "An unexpected exception occurred when creating schema: call_log"
            ) from e
        try:
            sql = SCHEMA["call_log_index1"]

            cursor.execute(sql)
        except Exception as e:
            raise RuntimeError(
                "An unexpected exception occurred when creating schema: call_log_index1"
            ) from e


def add_call_params(
    connection: ConnectionType,
    call_id: str,
    function_name: str,
    start_time: float,
    call_params: bytes,
) -> None:
    """Query add_call_params."""
    cursor = connection.cursor()
    try:
        sql = QUERY["add_call_params"]

        query_args = {
            "call_id": call_id,
            "function_name": function_name,
            "start_time": start_time,
            "call_params": call_params,
        }
        cursor.execute(sql, query_args)

    except Exception as e:
        raise RuntimeError(
            "An unexpected exception occurred while executing query: add_call_params"
        ) from e


def add_call_result(
    connection: ConnectionType, call_id: str, end_time: float, call_result: bytes
) -> None:
    """Query add_call_result."""
    cursor = connection.cursor()
    try:
        sql = QUERY["add_call_result"]

        query_args = {
            "call_id": call_id,
            "end_time": end_time,
            "call_result": call_result,
        }
        cursor.execute(sql, query_args)

    except Exception as e:
        raise RuntimeError(
            "An unexpected exception occurred while executing query: add_call_result"
        ) from e


def get_unfinished_calls(
    connection: ConnectionType,
) -> Iterable[GetUnfinishedCallsReturnType]:
    """Query get_unfinished_calls."""
    cursor = connection.cursor()
    try:
        sql = QUERY["get_unfinished_calls"]

        cursor.execute(sql)

        for row in cursor:
            row = GetUnfinishedCallsReturnType(
                call_id=row[0], function_name=row[1], call_params=row[2]
            )
            yield row
    except Exception as e:
        raise RuntimeError(
            "An unexpected exception occurred while executing query: get_unfinished_calls"
        ) from e


def get_call(connection: ConnectionType, call_id: str) -> Optional[GetCallReturnType]:
    """Query get_call."""
    cursor = connection.cursor()
    try:
        sql = QUERY["get_call"]

        query_args = {"call_id": call_id}
        cursor.execute(sql, query_args)

        row = cursor.fetchone()
        if row is None:
            return None
        else:
            return GetCallReturnType(
                function_name=row[0], call_params=row[1], call_result=row[2]
            )
    except Exception as e:
        raise RuntimeError(
            "An unexpected exception occurred while executing query: get_call"
        ) from e


def explain_queries() -> None:
    connection = apsw.Connection(":memory:")
    create_schema(connection)

    with connection:
        cursor = connection.cursor()

        try:
            sql = QUERY["add_call_params"]
            sql = "EXPLAIN " + sql

            query_args = {
                "call_id": None,
                "function_name": None,
                "start_time": None,
                "call_params": None,
            }
            cursor.execute(sql, query_args)

            print("Query add_call_params is syntactically valid.")
        except Exception as e:
            raise RuntimeError(
                "An unexpected exception occurred while executing query plan for: add_call_params"
            ) from e

        try:
            sql = QUERY["add_call_result"]
            sql = "EXPLAIN " + sql

            query_args = {"call_id": None, "end_time": None, "call_result": None}
            cursor.execute(sql, query_args)

            print("Query add_call_result is syntactically valid.")
        except Exception as e:
            raise RuntimeError(
                "An unexpected exception occurred while executing query plan for: add_call_result"
            ) from e

        try:
            sql = QUERY["get_unfinished_calls"]
            sql = "EXPLAIN " + sql

            cursor.execute(sql)

            print("Query get_unfinished_calls is syntactically valid.")
        except Exception as e:
            raise RuntimeError(
                "An unexpected exception occurred while executing query plan for: get_unfinished_calls"
            ) from e

        try:
            sql = QUERY["get_call"]
            sql = "EXPLAIN " + sql

            query_args = {"call_id": None}
            cursor.execute(sql, query_args)

            print("Query get_call is syntactically valid.")
        except Exception as e:
            raise RuntimeError(
                "An unexpected exception occurred while executing query plan for: get_call"
            ) from e


if __name__ == "__main__":
    explain_queries()
