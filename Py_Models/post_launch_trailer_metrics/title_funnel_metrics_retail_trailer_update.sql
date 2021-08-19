-- viewership table: source of viewership, either heartbeat or now_uer_stream
-- end_date: the end date of the viewership data

/*create or replace table {database}.{schema}.trailer_retail_view_percent (
      title_name varchar (255) not null
    , match_id varchar (255) not null
    , platform_name varchar (255) not null
    , nday_before integer
    , cumulative_day_num integer
    , total_trailer_num integer
    , match_id_platform varchar (255) not null
    , first_trailer_offered_timestamp timestamp
    , title_offered_timestamp timestamp
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
                from {database}.{schema}.trailer_retail_view_percent
                where 1=1
                    and nday_before = {nday_before}
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
        , edit_language
    from max_prod.catalog.asset_dim as asset
    join max_prod.catalog.asset_edit_dim as edit
        on asset.viewable_id = edit.viewable_id
    where 1 = 1
        and asset_title_short like '%Trailer%'
        and earliest_offered_timestamp is not null
        ),

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
    join {database}.{schema}.title_retail_funnel_metrics as f
        on t.trailer_title_name = get(split(f.title_name, ' S'),0)
            and t.earliest_offered_timestamp
                between dateadd(day, -56, f.earliest_offered_timestamp)
                    and dateadd(min, -1, f.earliest_offered_timestamp)
            and t.platform_name = f.platform_name
    left join existing_title_info as e
        on f.match_id= e.match_id
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

    trailer_match_id_agg as (
        select
              match_id
            , min(trailer_offered_timestamp) as first_trailer_offered_timestamp
            , min(title_offered_timestamp) as first_title_offered_timestamp
        from trailer_match_id
        group by 1
    ),

    max_viewership_match_id as (
                select
                      title_name
                    , f.match_id
                    , f.platform_name
                    , {nday_before}
                    , datediff(day,
                        greatest(first_trailer_offered_timestamp, dateadd(day, -28, title_offered_timestamp))
                            , dateadd(day, {nday_before}, title_offered_timestamp)) as cumulative_day_num
                    , first_trailer_offered_timestamp
                    , first_title_offered_timestamp
                    , count(distinct f.viewable_id) as total_trailer_num
                    , count(distinct hbo_uuid) as view_count
            from table ({viewership_table}) as v
            -- get trailer data
            join trailer_match_id as f
                on v.viewable_id = f.viewable_id
            -- get first trailer and title offered time
            join trailer_match_id_agg as agg
                on agg.match_id = f.match_id
            where 1=1
                and f.match_id is not null
                and stream_elapsed_play_seconds > 10
                and stream_min_timestamp_gmt between
                    greatest(first_trailer_offered_timestamp, dateadd(day, -28, first_title_offered_timestamp))
                        and dateadd(day, {nday_before}, first_title_offered_timestamp)
                and stream_min_timestamp_gmt >=
                    case when {viewership_table} in ('max_prod.viewership.max_user_stream','max_prod.viewership.max_user_stream_heartbeat_view') then '2020-05-27'
                        when {viewership_table} = 'max_prod.viewership.now_user_stream' then '2015-04-07'
                            end
                and dateadd(day, {nday_before}, first_title_offered_timestamp) < {end_date}
                and stream_min_timestamp_gmt <= {end_date}
                and first_trailer_offered_timestamp < first_title_offered_timestamp
                and cumulative_day_num > 0
            group by 1,2,3,4,5,6,7
            ),

    retail_sub_count_table as (
                select
                      first_trailer_offered_timestamp
                    , first_title_offered_timestamp
                    , count(distinct hbo_uuid) as total_retail_sub_count
                from max_viewership_match_id as t
                        -- the below table is modified from the max_prod.workspace.content_eval_first_sub_start table
                        -- created by Eileen Dise, the following are the modifications:
                        ------ 1). ignore the subscription gaps <= 24 hours, in order to create continous sub sessions
                        ------ 2). ignore the subcription sessions less than 24 hours
                left join {database}.{schema}.sub_period_in_uuid_test as a
                    -- give one day butter to both dates, since some titles may release in the late night
                    ------ logic: sessions without the following conditions
                    on a.subscription_expire_timestamp >= t.first_trailer_offered_timestamp
                        and a.subscription_start_timestamp <= dateadd(day, {nday_before}, first_title_offered_timestamp)
                where 1 = 1
                    and t.platform_name = a.platform_name
                group by 1,2
            ),

    last_table as (
                select
                      title_name
                    , match_id
                    , h.platform_name
                    , {nday_before} as nday_before
                    , cumulative_day_num
                    , total_trailer_num
                    , concat(case when h.platform_name = 'hboNow' then 0
                            else 1 end, '-', match_id) as match_id_platform
                    , h.first_trailer_offered_timestamp
                    , h.first_title_offered_timestamp
                    , view_count
                    , total_retail_sub_count
                    , {end_date}                              as last_update_timestamp
                    , view_count / total_retail_sub_count     as retail_trailer_view_metric
                from max_viewership_match_id as h
                left join retail_sub_count_table as r
                    on h.first_trailer_offered_timestamp = r.first_trailer_offered_timestamp
                        and h.first_title_offered_timestamp = r.first_title_offered_timestamp
            )

    select * from last_table
);



