-- viewership table: source of viewership, either heartbeat or now_uer_stream
-- end_date: the end date of the viewership data

/*create or replace table max_dev.workspace.trailer_retail_view_percent (
      trailer_title_name varchar (255) not null
    , title_name varchar (255) not null
    , match_id varchar (255) not null
    , platform_name varchar (255) not null
    , nday_before integer
    , cumulative_day_num integer
    , match_id_platform varchar (255) not null
    , min_trailer_offered_timestamp timestamp
    , title_offered_timestamp timestamp
-- use credits start time as the total runtime for now
    , viewe_count integer
    , total_retail_sub_count integer
    , last_update_timestamp timestamp
    , retail_trailer_view_metric float
    , constraint id_plt_session primary key (match_id, platform_name, match_id_platform)
    , constraint id_plt_session_unique unique (match_id, platform_name, match_id_platform)
)*/

insert into {database}.{schema}.trailer_retail_view_percent (
    with existing_title_info as (
                select distinct
                    match_id
                from {database}.{schema}.trailer_view_percent_test
                where 1=1
                    and platform_name =
                        case when {viewership_table} = 'max_prod.viewership.max_user_stream_heartbeat_view' then 'hboMax'
                            when {viewership_table} = 'max_prod.viewership.now_user_stream' then 'hboNow'
                                end
            ),

    title_info as (
        select distinct
        asset.viewable_id
        , case when asset_title_short like '%Season%'
            then trim(regexp_replace(get(split(asset_title_long, ': Season'),0), '"', ''))
            else trim(regexp_replace(get(split(asset_title_long, 'Trailer'),0), ':', ''))
            end as trailer_title_name
        , case when {viewership_table} in ('max_prod.viewership.max_user_stream', 'max_prod.viewership.max_user_stream_heartbeat_view') then first_offered_date_max
            when {viewership_table} = 'max_prod.viewership.now_user_stream' then first_offered_date_now
                end as earliest_offered_timestamp
        , case when {viewership_table} in ('max_prod.viewership.max_user_stream', 'max_prod.viewership.max_user_stream_heartbeat_view') then 'hboMax'
            when {viewership_table} = 'max_prod.viewership.now_user_stream' then 'hboNow'
                end as platform_name
        , runtime
        , edit_language
    from max_prod.catalog.asset_dim as asset
    join max_prod.catalog.asset_edit_dim as edit
        on asset.viewable_id = edit.viewable_id
    where 1 = 1
        and asset_title_short like '%Trailer%'
        and earliest_offered_timestamp is not null),

    trailer_match_id as (select distinct
        regexp_replace(trailer_title_name, 'Trailer', '') as trailer_title_name
        , title_name
        , t.viewable_id
        , f.match_id
        , t.platform_name
        , t.earliest_offered_timestamp as trailer_offered_timestamp
        , f.earliest_offered_timestamp as title_offered_timestamp
        , case when e.match_id is null then 0 else 1 end as exist_id
    from title_info as t
    join max_dev.workspace.title_retail_funnel_metrics as f
        on t.trailer_title_name = get(split(f.title_name, ' S'),0)
            and t.earliest_offered_timestamp
                between dateadd(day, -56, f.earliest_offered_timestamp)
                    and dateadd(min, -1, f.earliest_offered_timestamp)
            and t.platform_name = f.platform_name
    left join existing_title_info as e
        on f.match_id = e.match_id
    where 1=1
        and t.platform_name =
            (case
                when {viewership_table} in ('max_prod.viewership.max_user_stream', 'max_prod.viewership.max_user_stream_heartbeat_view')
                    then 'hboMax'
                when {viewership_table} = 'max_prod.viewership.now_user_stream'
                    then 'hboNow' end)
        and exist_id = {exist_ind_val}
        and trailer_offered_timestamp <= {end_date}
    order by trailer_title_name),

    max_viewership_match_id as (
                select
                trailer_title_name
                , title_name
                , match_id
                , f.platform_name
                , {nday_before}
                , datediff(day,
                    greatest(trailer_offered_timestamp, dateadd(day, -28, title_offered_timestamp))
                        , dateadd(day, {nday_before},title_offered_timestamp)) as cumulative_day_num
                , min(trailer_offered_timestamp) as min_trailer_offered_timestamp
                , max(title_offered_timestamp) as title_offered_timestamp
                -- use credits start time as the total runtime for now
                , count(distinct hbo_uuid) as viewe_count
            from table ({viewership_table}) as v
            -- get trailer data
            join trailer_match_id as f
                on v.viewable_id = f.viewable_id
            where 1=1
                and match_id is not null
                and stream_elapsed_play_seconds > 10
                and stream_min_timestamp_gmt between
                    greatest(trailer_offered_timestamp, dateadd(day, -28, title_offered_timestamp))
                        and dateadd(day, {nday_before}, title_offered_timestamp)
                and stream_min_timestamp_gmt >=
                    case when {viewership_table} in ('max_prod.viewership.max_user_stream','max_prod.viewership.max_user_stream_heartbeat_view') then '2020-05-27'
                        when {viewership_table} = 'max_prod.viewership.now_user_stream' then '2015-04-07'
                            end
                and stream_min_timestamp_gmt <= {end_date}
                and trailer_offered_timestamp < title_offered_timestamp
                and cumulative_day_num > 0
            group by 1,2,3,4,5,6
            ),

    retail_sub_count_table as (
                select
                        min_trailer_offered_timestamp
                    , title_offered_timestamp
                    , count(distinct hbo_uuid) as total_retail_sub_count
                from max_viewership_match_id as t
                        -- the below table is modified from the max_prod.workspace.content_eval_first_sub_start table
                        -- created by Eileen Dise, the following are the modifications:
                        ------ 1). ignore the subscription gaps <= 24 hours, in order to create continous sub sessions
                        ------ 2). ignore the subcription sessions less than 24 hours
                left join {database}.{schema}.sub_period_in_uuid_test as a
                    -- give one day butter to both dates, since some titles may release in the late night
                    ------ logic: sessions without the following conditions
                    on (((a.subscription_expire_timestamp <= t.min_trailer_offered_timestamp)
                        or (a.subscription_start_timestamp >= t.title_offered_timestamp)
                            ) = FALSE)
                where 1 = 1
                    and t.platform_name = a.platform_name
                group by 1,2
            ),

    last_table as (
                select
                    trailer_title_name
                    , title_name
                    , match_id
                    , h.platform_name
                    , {nday_before} as nday_before
                    , cumulative_day_num
                    , concat(case when h.platform_name = 'hboNow' then 0
                            else 1 end, '-', match_id) as match_id_platform
                    , h.min_trailer_offered_timestamp
                    , h.title_offered_timestamp
                -- use credits start time as the total runtime for now
                    , viewe_count
                    , total_retail_sub_count
                    , {end_date}                      as last_update_timestamp
                    , viewe_count / total_retail_sub_count     as retail_trailer_view_metric
                from max_viewership_match_id as h
                left join retail_sub_count_table as r
                    on h.min_trailer_offered_timestamp = r.min_trailer_offered_timestamp
                        and h.title_offered_timestamp = r.title_offered_timestamp
            )

select * from last_table
);



