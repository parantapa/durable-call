"""Test the call store module."""

import pickle
from datetime import datetime
from contextlib import closing

import apsw

import durable_call.call_store as call_store


def test_simple():
    con = apsw.Connection(":memory:")

    call_id = "call:1"
    function_name = "func1"
    params_pkl = pickle.dumps(dict(params=None))
    result_pkl = pickle.dumps(dict(result=None))

    with closing(con):
        call_store.create_schema(con)

        with con:
            start_time = datetime.utcnow().isoformat()
            call_store.add_call_params(
                con, call_id, function_name, start_time, params_pkl
            )

        with con:
            calls = list(call_store.get_unfinished_calls(con))
            assert len(calls) == 1
            assert calls[0].call_id == call_id

        with con:
            end_time = datetime.utcnow().isoformat()
            call_store.add_call_result(con, call_id, end_time, result_pkl)

        with con:
            calls = list(call_store.get_unfinished_calls(con))
            assert len(calls) == 0

        with con:
            match call_store.get_call(con, call_id):
                case None:
                    raise RuntimeError("Should not happen")
                case ret:
                    assert ret.function_name == function_name
                    assert ret.call_params == params_pkl
                    assert ret.call_result == result_pkl


def test_empty():
    con = apsw.Connection(":memory:")

    with closing(con):
        call_store.create_schema(con)

        with con:
            calls = list(call_store.get_unfinished_calls(con))
            assert len(calls) == 0

        with con:
            match call_store.get_call(con, "call:1"):
                case None:
                    pass
                case other:
                    raise RuntimeError("Should not happen: %r" % other)
