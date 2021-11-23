-- nday: [ndays] since first offered
-- day_latency: started counting after [day_latency] days
-- viewership table: source of viewership, either heartbeat or now_uer_stream
-- sub_table: subscriber table, should be updated everytime before updating the heartbeat
-- end_date: the end date of the viewership data
-- exist_ind_val: indicating if a title_name - platform_name - days_since_first_offered combination exists in the target table

insert into {database}.{schema}.title_retail_funnel_metrics (
    with existing_title_info as (
        select distinct
              match_id
        from {database}.{schema}.title_retail_funnel_metrics
        where 1=1
            and days_since_first_offered = {nday}
            and platform_name =
                case when {viewership_table} = 'max_prod.viewership.max_user_stream_heartbeat_view' then 'hboMax'
                    when {viewership_table} = 'max_prod.viewership.now_user_stream' then 'hboNow'
                        end
    ),

    title_basic_info as (
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
            -- this version we consider en version data only
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
                , case when e.match_id is null then 0 else 1 end as exist_ind
            from title_basic_info as t
            left join existing_title_info as e
                on t.match_id = e.match_id
            where exist_ind = {exist_ind_val}
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
                  -- for titles were not on linear TV or in theater before,
                  -- use the first offered date on Now or/and Max
                  , min(fst_public_timestamp) as earliest_public_timestamp
             from title_basic_info_new_titles
             group by 1, 2
             having datediff(minute, earliest_offered_timestamp, least({end_date}, dateadd(day, -{day_latency}, convert_timezone('GMT',current_timestamp())))) >= 1440*{nday}
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
                , dateadd(day, {nday}, earliest_offered_timestamp) as end_consideration_timestamp
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
            , coalesce(credits_start_time, runtime) as runtime
            , hbo_uuid
            , stream_elapsed_play_seconds
            , e.earliest_offered_timestamp
            , e.last_offered_timestamp
            -- assuming all the NOW titles were on air or in theater before
            , earliest_public_timestamp
            , stream_min_timestamp_gmt
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
         join offered_date_pairs as o
            on e.earliest_offered_timestamp = o.earliest_offered_timestamp
         where 1=1
            and match_id is not null
            and stream_min_timestamp_gmt between o.earliest_offered_timestamp and o.end_consideration_timestamp
            and stream_min_timestamp_gmt >= first_offered_timestamp_on_platform
            and stream_min_timestamp_gmt >=
                case when {viewership_table} in ('max_prod.viewership.max_user_stream','max_prod.viewership.max_user_stream_heartbeat_view') then '2020-05-27'
                    when {viewership_table} = 'max_prod.viewership.now_user_stream' then '2015-04-07'
                        end
            and stream_min_timestamp_gmt <= {end_date}
            -- the US filter is in the subscription table query, so this only the US users are considered in this version
         ),

         max_retail_viewership_base_info as (
             select
                    title_name
                  , match_id
                  , season_number_adj
                  , content_category
                  , single_episode_ind
                  , v.earliest_offered_timestamp
                  , earliest_public_timestamp
                  , last_offered_timestamp
                  , a.hbo_uuid
                  , viewable_id
                  , max(stream_elapsed_play_seconds) / 3600                      as max_played_hours
                  , max(runtime) / 3600                                          as run_time_hours
                  -- constrain the daily total viewing hours less than or equal to
                  -- the total available hours in episodic level
                  -- worth to mention, while explaining to people
                  , least(sum(stream_elapsed_play_seconds), max(runtime)) / 3600 as streamed_hours
                  , case when sum(stream_elapsed_play_seconds) > 0.9 * max(runtime) then 1
                        else 0
                            end as episode_completion_ind
             from max_viewership_match_id as v
             join offered_date_pairs as o
                on v.earliest_offered_timestamp = o.earliest_offered_timestamp
             join {database}.{schema}.sub_period_in_uuid_test as a
                            on (((a.subscription_expire_timestamp < o.earliest_offered_timestamp)
                                or (a.subscription_start_timestamp >= o.end_consideration_timestamp)
                                ) = FALSE)
                                and a.hbo_uuid = v.hbo_uuid
                                and stream_min_timestamp_gmt>=subscription_start_timestamp
             where 1 = 1
             group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
         ),

         max_retail_viewership_agg_lv as (
             select
                    title_name
                  , match_id
                  , case when {viewership_table} in ('max_prod.viewership.max_user_stream','max_prod.viewership.max_user_stream_heartbeat_view') then 'hboMax'
                        when {viewership_table} = 'max_prod.viewership.now_user_stream' then 'hboNow'
                            end as platform_name
                  , hbo_uuid
                  , mode(season_number_adj) as season_number_adj
                  , mode(content_category) as content_category
                  , mode(single_episode_ind) as single_episode_ind
                  , min(earliest_offered_timestamp) as earliest_offered_timestamp
                  , max(last_offered_timestamp) as last_offered_timestamp
                  , min(earliest_public_timestamp) as earliest_public_timestamp
                  , sum(streamed_hours) as total_played_hours
                  , sum(episode_completion_ind) as total_num_completed_episodes
             from max_retail_viewership_base_info
             group by 1, 2, 3, 4
             having total_played_hours is not null and total_played_hours > 0
         ),

-- Subcription Count Table: count the number of subs (uuid) that had the chance to play the season
         retail_sub_count_table as (
             select
                    o.earliest_offered_timestamp
                  , count(distinct hbo_uuid) as total_retail_sub_count
             from offered_date_pairs as o
             left join {database}.{schema}.sub_period_in_uuid_test as a
                on (((a.subscription_expire_timestamp <= o.earliest_offered_timestamp)
                    or (a.subscription_start_timestamp >= o.end_consideration_timestamp)
                        ) = FALSE)
             where 1 = 1
                and platform_name = (case when {viewership_table} in ('max_prod.viewership.max_user_stream','max_prod.viewership.max_user_stream_heartbeat_view')
                                            then 'hboMax'
                                        when {viewership_table} = 'max_prod.viewership.now_user_stream'
                                            then 'hboNow'
                                        end)
             group by 1
         ),

-- Final Process Tables
         retail_sum_hours as (
             select
                    title_name
                  , v.match_id
                  , season_number_adj
                  , content_category
                  , single_episode_ind
                  , platform_name
                  , earliest_offered_timestamp
                  , last_offered_timestamp
                  , earliest_public_timestamp
                  , hbo_uuid
                  , case when total_played_hours > 0 then 1 else 0 end
                                                                      as retail_played_ind_total
                  , case when (((total_played_hours > 2/60) and single_episode_ind = 1) or     -- movies & single episodes
                              ((total_num_completed_episodes > 0) and single_episode_ind = 0)) -- series
                        then 1 else 0 end
                                                                      as retail_view_ind_total
                  -- since each of the episode required more than 90% streamed time
                  -- this is equaivalent to the 90% total run time
                  , case when viewable_id_count = total_num_completed_episodes
                        then 1 else 0 end
                                                                      as retail_completion_ind_total
             from max_retail_viewership_agg_lv as v
             left join title_total_episode_full_lv as t
                on v.match_id = t.match_id
         ),

         last_table as (
             select
                    title_name
                  , h.match_id
                  , season_number_adj
                  , content_category
                  , single_episode_ind
                  , platform_name
                  , h.earliest_offered_timestamp
                  , last_offered_timestamp
                  , earliest_public_timestamp
                  , {nday}                                     as days_since_first_offered
                  , total_retail_sub_count
                  , {end_date}                       as last_update_timestamp
                  , count(*)                                  as retail_played_count
                  , sum(retail_view_ind_total)                       as retail_viewed_count
                  , sum(retail_completion_ind_total)                 as retail_completion_count
                  , retail_played_count / total_retail_sub_count     as retail_played_count_percent
                  , retail_viewed_count / total_retail_sub_count     as retail_viewed_count_percent
             from retail_sum_hours as h
             left join retail_sub_count_table as r
                on h.earliest_offered_timestamp = r.earliest_offered_timestamp
             group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
         )

    select
        *
    from last_table
);