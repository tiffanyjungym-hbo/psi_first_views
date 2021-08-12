-- nday: [ndays] since first offered
-- day_latency: started counting after [day_latency] days
-- end_date: the end date of the viewership data
-- exist_ind_val: indicating if a title_name - platform_name - days_since_first_offered combination exists in the target table

insert into {database}.{schema}.title_retail_funnel_metrics (
    with existing_title_info as (
            select distinct
                  match_id
            from {database}.{schema}.title_retail_funnel_metrics
            where 1=1
                and days_since_first_offered = 0
                and platform_name =
                    case when {viewership_table} = 'max_prod.viewership.max_user_stream_heartbeat_view' then 'hboMax'
                        when {viewership_table} = 'max_prod.viewership.now_user_stream' then 'hboNow'
                            end
        ),

    title_basic_info as (
            select distinct
                  series_id
                , asset.viewable_id
                , case
                    when (theatrical_release_date is null and original_linear_air_date is null)
                        then (case when (first_offered_date_max is null or first_offered_date_now is null)
                                        then coalesce(first_offered_date_max, first_offered_date_now)
                                    else least(first_offered_date_max, first_offered_date_now)
                                        end)
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
                , case when {viewership_table}  in ('max_prod.viewership.max_user_stream', 'max_prod.viewership.max_user_stream_heartbeat_view') then first_offered_date_max
                    when {viewership_table}  = 'max_prod.viewership.now_user_stream' then first_offered_date_now
                        end as first_offered_timestamp_on_platform
                , case when {viewership_table}  in ('max_prod.viewership.max_user_stream', 'max_prod.viewership.max_user_stream_heartbeat_view') then 'hboMax'
                    when {viewership_table}  = 'max_prod.viewership.now_user_stream' then 'hboNow'
                        end as platform_name
            from max_prod.catalog.asset_dim as asset
            where 1 = 1
                and title_name is not null
                and first_offered_timestamp_on_platform is not null
                and asset_type in ('FEATURE', 'ELEMENT')
                and content_category is not null
        ),

            title_basic_info_prelaunch_titles as (
                select
                      title_name
                    , match_id
                    , season_number_adj
                    , content_category
                    , single_episode_ind
                    , platform_name
                    , min(first_offered_timestamp_on_platform) as earliest_offered_timestamp
                    , max(first_offered_timestamp_on_platform) as last_offered_timestamp
                    , min(fst_public_timestamp) as earliest_public_timestamp
                from title_basic_info as t
                group by 1, 2, 3, 4, 5,  6
                having earliest_offered_timestamp between {end_date} and dateadd(day, 90, {end_date})
             ),

            title_filter as (
            select
                t.*
                , case when e.match_id is null then 0 else 1 end as exist_ind
            from title_basic_info_prelaunch_titles as t
            left join existing_title_info as e
                on t.match_id = e.match_id
            where 1=1
                and exist_ind = {exist_ind_val}
            ),

            last_table as (
                 select
                        title_name
                      , match_id
                      , season_number_adj
                      , content_category
                      , single_episode_ind
                      , platform_name
                      , earliest_offered_timestamp
                      , last_offered_timestamp
                      , earliest_public_timestamp
                      , 0                      as days_since_first_offered
                      , null as total_retail_sub_count
                      , {end_date}                 as last_update_timestamp
                      , null                       as retail_played_count
                      , null                       as retail_viewed_count
                      , null                       as retail_completion_count
                      , null                       as retail_played_count_percent
                      , null                       as retail_viewed_count_percent
                 from title_filter as t
                 order by earliest_offered_timestamp
             )

        select
            *
        from last_table
);