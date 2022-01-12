-- run_date: YYYY-MM-DD string for run date
-- database: db of table
-- schema: schema of table

with fv as (
    select
          ba.title_id
        , ba.title_name
        , ba.season_number
        , ba.content_category
        , ba.category
        , tier
        , request_time_gmt::date as request_date
        , count(distinct concat(hbo_uuid, subscription_id)) as first_views
    from max_prod.bi_analytics.subscription_first_content_watched a
    inner join {database}.{schema}.psi_past_base_assets ba
        on a.viewable_id = ba.viewable_id
    where 1 = 1
        and request_time_gmt::date between asset_max_premiere and asset_max_end_dt
        and request_time_gmt::date between effective_start_date and effective_end_date
        and request_time_gmt::date < dateadd('days',-1,{run_date})
        and country_iso_code in ('US','PR','GU')
    group by 1,2,3,4,5,6,7
)
, hv as (
    select
          ba.title_id
        , ba.title_name
        , ba.season_number
        , ba.content_category
        , ba.category
        , tier
        , request_time_gmt::date as request_date
        , coalesce(round(sum(stream_elapsed_play_seconds)/3600,3), 0) as hours_viewed
    from max_prod.viewership.max_user_stream_heartbeat a
    inner join {database}.{schema}.psi_past_base_assets ba
        on a.viewable_id = ba.viewable_id
    where 1 = 1
    and stream_elapsed_play_seconds >= 120
    and request_time_gmt > '2020-05-27 07:00:00'
    and request_time_gmt::date between asset_max_premiere and asset_max_end_dt
    and request_time_gmt::date between effective_start_date and effective_end_date
    and request_time_gmt::date < dateadd('days',-1,{run_date})
    group by 1,2,3,4,5,6,7
)
, dates as (
    select distinct
          ba.title_id
        , ba.title_name
        , ba.season_number
        , ba.content_category
        , ba.content_source
        , ba.program_type
        , ba.category
        , ba.tier
        , ba.effective_start_date
        , request_date
        , case when request_date::date = effective_start_date::date then 1 else 0 end as premiere_ind
        , count(distinct case when request_date::date = asset_max_premiere::date then viewable_id else null end) as asset_premiere_count
        , round(sum(distinct case when request_date::date = asset_max_premiere::date then asset_run_time else 0 end)/3600,3) as premiering_hours_runtime
    from {database}.{schema}.psi_past_base_assets ba
    cross join (
        select distinct seq_date as request_date 
        from max_prod.staging.date_range 
        where seq_date < '2024-12-31'::date
    ) rd
    where rd.request_date between 
    coalesce(ba.effective_start_date,ba.season_premiere,ba.asset_max_premiere) 
    and dateadd('days',90,coalesce(ba.effective_start_date,ba.season_premiere,ba.asset_max_premiere))
      and rd.request_date between effective_start_date and effective_end_date
    group by 1,2,3,4,5,6,7,8,9,10,11
    order by 2,3,8
)
    select dt.*
        , coalesce(first_views,0) as first_views
        , coalesce(hours_viewed,0) as hours_viewed
        , dt.request_date - effective_start_date as days_since_premiere
        , {run_date} - effective_start_date -1 as days_on_platform
        , case when {run_date} - effective_start_date - 1 >=
            case when dt.category = 'Popcorn' and year(effective_start_date) < 2022 then 31 else 90 end
        then 1 else 0 end as finished_window_flag
    from dates dt
    left join hv
        on dt.title_id = hv.title_id
        and dt.season_number = hv.season_number
        and dt.request_date = hv.request_date
        and dt.content_category = hv.content_category
        and dt.category = hv.category
        and dt.tier = hv.tier
    left join fv
        on dt.title_id = fv.title_id
        and dt.season_number = fv.season_number
        and dt.request_date = fv.request_date
        and dt.content_category = fv.content_category
        and dt.category = fv.category
        and dt.tier = fv.tier
    where 1 = 1
    order by title_id, title_name, season_number, category, request_date
;