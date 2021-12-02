select distinct
      to_char(date(max(last_update_timestamp))) as latest_update_timestamp
from {database}.{schema}.title_retail_funnel_metrics
where 1=1
    and platform_name =
        case when  {viewership_table} = 'max_prod.viewership.max_user_stream_heartbeat_view' then 'hboMax'
            when  {viewership_table} = 'max_prod.viewership.now_user_stream' then 'hboNow'
                end