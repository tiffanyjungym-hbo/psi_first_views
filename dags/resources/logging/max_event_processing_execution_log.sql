-- Script used to insert the execution log details after successful query execution.This script is called
-- upon after every successful DDL/DML execution over Snowflake and logs execution stats details pertaining to
-- current user session in Snowflake.

INSERT INTO utility.event_processing_execution_log
 SELECT
    '{dag_id}',
    '{task_id}',
    '{execution_date}'::date,
    session_id,
    query_id,
    query_text,
    database_name,
    schema_name,
    query_type,
    warehouse_name,
    warehouse_size,
    warehouse_type,
    cluster_number,
    execution_status,
    error_code,
    error_message,
    start_time,
    end_time,
    total_elapsed_time,
    bytes_scanned,
    rows_produced,
    compilation_time,
    execution_time,
    queued_provisioning_time,
    queued_repair_time,
    queued_overload_time,
    transaction_blocked_time
  FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
  where query_text not like 'insert into utility.event_processing_execution_log%';
