# Data Store
-- module: call_store

-- schema: call_log

create table if not exists call_log (
    call_id text primary key,
    function_name text not null,
    start_time real not null,
    call_params blob not null,

    end_time real,
    call_result blob
) ;

-- schema: call_log_index1

create index if not exists call_log_index1
on call_log (call_result)
where call_result is null ;

-- query: add_call_params
-- params: call_id: str!, function_name: str!, start_time: float!, call_params: bytes!

insert into call_log values (
    :call_id, :function_name, :start_time, :call_params, null, null
) ;

-- query: add_call_result
-- params: call_id: str!, end_time: float!, call_result: bytes!

update call_log
set end_time = :end_time, call_result = :call_result
where call_id = :call_id ;

-- query: get_unfinished_calls
-- return*: call_id: str!, function_name: str!, call_params: bytes!

select call_id, function_name, call_params
from call_log
where call_result is null
order by start_time asc ;

-- query: get_call
-- params: call_id: str!
-- return?: function_name: str!, call_params: bytes!, call_result: bytes

select function_name, call_params, call_result
from call_log
where call_id = :call_id ;
