-- day_latency: started counting after [day_latency] days
-- viewership table: source of viewership, either heartbeat or now_uer_stream
-- end_date: the end date of the viewership data

create or replace table {database}.{schema}.title_funnel_metrics_base_table as (
    with title_basic_info as (
            select distinct
                  series_id
                , asset.viewable_id
                -- deal with air_date and theaterical date first
                , case
                    when (theatrical_release_date is null and original_linear_air_date is null)
                    -- if both linear and theatrical dates are null, then take the minimum of max and now date
                        then (case when (first_offered_date_max is null or first_offered_date_now is null)
                                        then coalesce(first_offered_date_max, first_offered_date_now)
                                    else least(first_offered_date_max, first_offered_date_now)
                                        end)
                    -- if just one of them is null
                    when (theatrical_release_date is null or original_linear_air_date is null)
                        then coalesce(theatrical_release_date, original_linear_air_date)
                    else least(theatrical_release_date, original_linear_air_date)
                        end as fst_public_timestamp
                , case when content_category = 'series'
                    then ifnull(season_number, 1)
                    else season_number
                        end as season_number_adj
                , coalesce(concat(series_title_long, ' S', season_number_adj), asset_title_long) as title_name
                , coalesce(concat(series_id, '-', season_number_adj), asset.viewable_id) as match_id
                , asset.content_category
                , case when series_id is null then 1 else 0
                    end as single_episode_ind
                -- left the season qualidifier seperately for now, thinking to change it soon
                , case when {viewership_table} in ('max_prod.viewership.max_user_stream', 'max_prod.viewership.max_user_stream_heartbeat_view') then first_offered_date_max
                    when {viewership_table} = 'max_prod.viewership.now_user_stream' then first_offered_date_now
                        end as first_offered_timestamp_on_platform
            from max_prod.catalog.asset_dim as asset
            where 1 = 1
                and title_name is not null
                and first_offered_timestamp_on_platform is not null
                and asset_type in ('FEATURE', 'ELEMENT')
                and first_offered_timestamp_on_platform <= least(({end_date}), dateadd(day, -{day_latency}, convert_timezone('GMT',current_timestamp())))
                and content_category is not null
                and content_category in ('movies', 'series', 'special')
             ),

             title_basic_info_new_titles as (
                select
                      series_id
                    , viewable_id
                    , fst_public_timestamp
                    , season_number_adj
                    , title_name
                    , t.match_id
                    , content_category
                    , single_episode_ind
                    , first_offered_timestamp_on_platform
                    --, case when e.match_id is null then 0 else 1 end as exist_ind
                from title_basic_info as t
             ),

             earliest_date as (
                 select
                        title_name
                      , match_id
                      , mode(season_number_adj) as season_number_adj
                      , mode(content_category) as content_category
                      , mode(single_episode_ind) as single_episode_ind
                      , min(first_offered_timestamp_on_platform) as earliest_offered_timestamp
                      , max(first_offered_timestamp_on_platform) as last_offered_timestamp
                      , min(fst_public_timestamp) as earliest_public_timestamp
                 from title_basic_info_new_titles
                 group by 1, 2
                 having datediff(day, earliest_offered_timestamp, least({end_date}, dateadd(day, -{day_latency}, convert_timezone('GMT',current_timestamp())))) >= 0
             ),

             episode_fst_offered_date as (
                 select distinct
                      viewable_id
                    , first_offered_timestamp_on_platform
                 from title_basic_info_new_titles
             ),

    -- Asset Available Hour Tables
    ---- 1. title_add_hours_season_lv: episode release dates and the corresponding new added hour
             title_total_episode_full_lv as (
                 select
                        match_id
                      , count(distinct viewable_id) as viewable_id_count
                 from title_basic_info_new_titles
                 group by 1
             ),

    -- earliest offered dates and nth days after offered dates only
            offered_date_pairs as (
                select distinct
                      earliest_offered_timestamp
                    , dateadd(day, 28, earliest_offered_timestamp) as end_consideration_timestamp
                    from (select
                                match_id
                                , min(earliest_offered_timestamp) as earliest_offered_timestamp
                        from earliest_date
                        group by 1)
            ),

    -- Played Hour Tables
             max_viewership_match_id as (
                select
                  title_name
                , match_id
                , e.season_number_adj
                , v.viewable_id
                , content_category
                , single_episode_ind
                -- use credits start time as the total runtime for now
                , credits_start_time as runtime
                , hbo_uuid
                , stream_min_timestamp_gmt
                , stream_elapsed_play_seconds
                , e.earliest_offered_timestamp
                , e.last_offered_timestamp
                -- assuming all the NOW titles were on air or in theater before
                , earliest_public_timestamp
             from table ({viewership_table}) as v
             join earliest_date as e
                on coalesce(concat(series_id, '-', v.season_number), viewable_id) = e.match_id
             -- to get earliest offer data
             join episode_fst_offered_date as f
                on v.viewable_id = f.viewable_id
             -- to getting the runtime from a consolidated source
             join max_prod.catalog.asset_edit_dim as edit
                on edit.viewable_id = v.viewable_id
                    and edit.edit_id = v.edit_id
             -- to get the earliest offered date 
             join offered_date_pairs as o
                on e.earliest_offered_timestamp = o.earliest_offered_timestamp
             where 1=1
                and match_id is not null
                -- consider the viewership between the start and 28 days after start timestamp of a title-season level
                and stream_min_timestamp_gmt between o.earliest_offered_timestamp and o.end_consideration_timestamp
                -- cant stream an episode or movie before it is launched
                and stream_min_timestamp_gmt >= first_offered_timestamp_on_platform
                and stream_min_timestamp_gmt >=
                    case when {viewership_table} in ('max_prod.viewership.max_user_stream','max_prod.viewership.max_user_stream_heartbeat_view') then '2020-05-27'
                        when {viewership_table} = 'max_prod.viewership.now_user_stream' then '2015-04-07'
                            end
                -- consider en-US viewerships only for this version
             ),

    max_retail_viewership_base_info as (
             select
                    title_name
                  , v.match_id
                  , season_number_adj
                  , content_category
                  , single_episode_ind
                  , v.earliest_offered_timestamp
                  , earliest_public_timestamp
                  , last_offered_timestamp
                  , a.hbo_uuid
                  , stream_min_timestamp_gmt
                  , viewable_id
                  , viewable_id_count
                  , runtime/3600 as runtime_hr_duration
                  , stream_elapsed_play_seconds/3600 as stream_hr_duration
             from max_viewership_match_id as v
             join offered_date_pairs as o
                on v.earliest_offered_timestamp = o.earliest_offered_timestamp
             join {database}.{schema}.sub_period_in_uuid_test as a
               -- consider the hbo_uuids active within the period only
                            on a.subscription_expire_timestamp >= o.earliest_offered_timestamp
                                and a.subscription_start_timestamp < o.end_consideration_timestamp
                                and a.hbo_uuid = v.hbo_uuid
               -- get total number of episodes 
             join title_total_episode_full_lv as l
                on l.match_id = v.match_id
             where 1 = 1
    )

    select
        *
    from max_retail_viewership_base_info
)