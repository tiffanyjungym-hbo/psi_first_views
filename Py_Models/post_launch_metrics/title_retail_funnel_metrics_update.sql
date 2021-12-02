-- nday: [ndays] since first offered
-- day_latency: started counting after [day_latency] days
-- viewership table: source of viewership, either heartbeat or now_uer_stream
-- end_date: the end date of the viewership data
-- exist_ind_val: indicating if a title_name - platform_name - days_since_first_offered combination exists in the target table

insert into {database}.{schema}.title_retail_funnel_metrics (
    with existing_title_info as (
        select distinct
              match_id
            , earliest_offered_timestamp
        from {database}.{schema}.title_retail_funnel_metrics
        where 1=1
            and days_since_first_offered = $nday
            and platform_name =
                case when $viewership_table = 'max_prod.viewership.max_user_stream_heartbeat_view' then 'hboMax'
                    when $viewership_table = 'max_prod.viewership.now_user_stream' then 'hboNow'
                        end
        ),

        viewership_title_info as (
            select distinct
                match_id
            from {database}.{schema}.title_funnel_metrics_base_table
            where 1=1
                and stream_min_timestamp_gmt between earliest_offered_timestamp
                    and dateadd(day, $nday, earliest_offered_timestamp)
                and dateadd(day, $nday, earliest_offered_timestamp) <=
                    least(($end_date), dateadd(day, -$day_latency, convert_timezone('GMT',current_timestamp())))
            ),

        match_id_for_update as (
            select distinct
                v.match_id
                , case when e.match_id is null then 1 else 0 end as exist_ind
            from existing_title_info as e
            right join viewership_title_info as v
                on e.match_id = v.match_id
            where 1=1
                and exist_ind = 1
        ),

        max_retail_viewership_base_info as (
                select
                      title_name
                    , v.match_id
                    , season_number_adj
                    , content_category
                    , single_episode_ind
                    , v.earliest_offered_timestamp
                    , dateadd(day, $nday, v.earliest_offered_timestamp) as end_consideration_date
                    , earliest_public_timestamp
                    , last_offered_timestamp
                    , a.hbo_uuid
                    , viewable_id
                    , max(viewable_id_count) as viewable_id_count
                    -- constrain the daily total viewing hours less than or equal to
                    -- the total available hours in episodic level
                    -- worth to mention, while explaining to people
                    , max(runtime_hr_duration)                                 as run_time_hours
                    , least(sum(stream_hr_duration), max(runtime_hr_duration)) as streamed_hours
                    , case when sum(stream_hr_duration) > 0.9 * max(runtime_hr_duration) then 1
                            else 0
                                end as episode_completion_ind
                from {database}.{schema}.title_funnel_metrics_base_table as v
                join {database}.{schema}.sub_period_in_uuid_test as a
                    on a.subscription_expire_timestamp >= v.earliest_offered_timestamp
                        and a.subscription_start_timestamp <= dateadd(day, $nday, v.earliest_offered_timestamp)
                        and a.hbo_uuid = v.hbo_uuid
                join match_id_for_update as u
                    on v.match_id = u.match_id
                where 1 = 1
                    and stream_min_timestamp_gmt between earliest_offered_timestamp
                        and dateadd(day, $nday, earliest_offered_timestamp)
                group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        ),

        max_retail_viewership_agg_lv as (
             select
                    title_name
                  , match_id
                  , case when $viewership_table in ('max_prod.viewership.max_user_stream','max_prod.viewership.max_user_stream_heartbeat_view') then 'hboMax'
                        when $viewership_table = 'max_prod.viewership.now_user_stream' then 'hboNow'
                            end as platform_name
                  , hbo_uuid
                  , mode(season_number_adj) as season_number_adj
                  , mode(content_category) as content_category
                  , mode(single_episode_ind) as single_episode_ind
                  , min(earliest_offered_timestamp) as earliest_offered_timestamp
                  , max(last_offered_timestamp) as last_offered_timestamp
                  , min(earliest_public_timestamp) as earliest_public_timestamp
                  , max(run_time_hours) as run_time_hours
                  , sum(streamed_hours) as total_played_hours
                  , sum(episode_completion_ind) as total_num_completed_episodes
                  , max(viewable_id_count) as viewable_id_count
             from max_retail_viewership_base_info
             group by 1, 2, 3, 4
             having total_played_hours is not null and total_played_hours > 0
         ),

    -- Subcription Count Table: count the number of subs (uuid) that had the chance to play the season
         retail_sub_count_table as (
             select
                    earliest_offered_timestamp
                  , count(distinct hbo_uuid) as total_retail_sub_count
             from (
                select distinct
                      earliest_offered_timestamp
                from {database}.{schema}.title_funnel_metrics_base_table
                    ) as o
             left join {database}.{schema}.sub_period_in_uuid_test as a
                 -- consider the active SVOD hbo_uuids within the consideration period only
                on a.subscription_expire_timestamp >= o.earliest_offered_timestamp
                    and a.subscription_start_timestamp <= dateadd(day, $nday, o.earliest_offered_timestamp)
             where 1 = 1
                and platform_name = (case when $viewership_table in ('max_prod.viewership.max_user_stream','max_prod.viewership.max_user_stream_heartbeat_view')
                                            then 'hboMax'
                                        when $viewership_table = 'max_prod.viewership.now_user_stream'
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
                  , case when (((total_played_hours > least(2/60, run_time_hours)) and single_episode_ind = 1) or     -- movies & single episodes
                              ((total_num_completed_episodes > 0) and single_episode_ind = 0)) -- series
                        then 1 else 0 end
                                                                      as retail_view_ind_total
                  -- since each of the episode required more than 90% streamed time
                  -- this is equaivalent to the 90% total run time
                  , case when viewable_id_count = total_num_completed_episodes
                        then 1 else 0 end as retail_completion_ind_total
             from max_retail_viewership_agg_lv as v
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
                  , $nday                                     as days_since_first_offered
                  , total_retail_sub_count
                  , $end_date                       as last_update_timestamp
                  , count(*)                                  as retail_played_count
                  , sum(retail_view_ind_total)                       as retail_viewed_count
                  , sum(retail_completion_ind_total)                 as retail_completion_count
                  -- the denominator should be based on impression counts, but use view counts instead for now
                  ---- (impression counts not available)
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