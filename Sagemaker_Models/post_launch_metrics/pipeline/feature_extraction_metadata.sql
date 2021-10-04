USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/metadata_feature.csv
FROM (
    with runtime_info as (
        select
            viewable_id
            -- different language version has different length,
            -- just take the average for simplicity
            , avg(credits_start_time) as credits_start_time
        from max_prod.catalog.asset_edit_dim as edit
        group by 1
    ),

    metadata as (
        select distinct
            coalesce(concat(series_id, '-', case when asset.content_category = 'series'
                then ifnull(asset.season_number, 1)
                else asset.season_number
                    end), asset.viewable_id) as match_id
            , case when content_category = 'series'
                    then ifnull(season_number, 1)
                    else ifnull(season_number, -1)
                        end as season_number_adj
            , asset.viewable_id
            , case when program_type is null then 'unknown' else program_type end as program_type
            , asset.content_category
            , credits_start_time
            , release_year
            , licensor
            , descriptive_genre_desc
            , wm_enterprise_genres
            , navigation_genre_desc
            , (case when first_offered_date_hbo is null then to_timestamp('2120-05-27') else first_offered_date_hbo end)
                as  first_offered_date_hbo
            , (case when original_linear_air_date is null then to_timestamp('2120-05-27') else original_linear_air_date end)
                as  original_linear_air_date
            , (case when theatrical_release_date is null then to_timestamp('2120-05-27') else theatrical_release_date end)
                as  theatrical_release_date
            , (case when first_offered_date_max is null then to_timestamp('2120-05-27') else first_offered_date_max end)
                as  first_offered_date_max
            , (case when first_offered_date_now is null then to_timestamp('2120-05-27') else first_offered_date_now end)
                as first_offered_date_now
            , (case when premiere_air_date is null then to_timestamp('2120-05-27') else premiere_air_date end)
                as premiere_air_date
    from max_prod.catalog.asset_dim as asset
    -- to get runtime data
    left join runtime_info as edit
        on asset.viewable_id = edit.viewable_id
    ),

    meta_agg as (
        select
            m.match_id
            , season_number_adj
            , mode(m.program_type) as program_type
            , mode(m.content_category) as content_category
            , ifnull(sum(credits_start_time)/3600.0,-1) as total_hours
            , min(release_year) as prod_release_year
            , listagg(distinct lower(licensor), '|') as licensor_agg
            , listagg(distinct lower(descriptive_genre_desc), '|') as descriptive_genre_desc_agg
            , listagg(distinct lower(wm_enterprise_genres), '|') as wm_enterprise_genres_agg
            , listagg(distinct lower(navigation_genre_desc), '|') as navigation_genre_desc_agg
            , min(least(first_offered_date_hbo,
                original_linear_air_date,
                theatrical_release_date,
                first_offered_date_max,
                first_offered_date_now,
                premiere_air_date))
                    as earliest_public_timestamp
        from metadata as m
        group by 1,2
    ),

    fin_meta_data_tmp as (
        select distinct
            title_name
            , m.match_id
            , season_number_adj
            , earliest_offered_timestamp
            , program_type
            , m.content_category
            , case when dateadd(day, 28, earliest_offered_timestamp) <= last_offered_timestamp then 1 else 0 end
                as in_sequantial_releasing_period
            , case when year(earliest_offered_timestamp) = prod_release_year
                then  1 else 0
                    end as at_release_year
            , dayofweek(earliest_offered_timestamp) as dayofweek_earliest_date
            , single_episode_ind
            , total_hours
            , prod_release_year
            , licensor_agg
            , descriptive_genre_desc_agg
            , wm_enterprise_genres_agg
            , navigation_genre_desc_agg
            , concat(case when platform_name = 'hboNow' then 0
                        else 1 end, '-', m.match_id) as match_id_platform
            , case when platform_name = 'hboNow' then 0
                        else 1 end as platform_name
            , datediff(day, m.earliest_public_timestamp, earliest_offered_timestamp)/365.0 as title_age_approx_meta
            , datediff(year, to_date(to_char(prod_release_year), 'YYYY'), earliest_offered_timestamp) as title_age_approx_imdb
            , case when p.viewable_id is null then 0 else 1 end as popcorn_ind
        from meta_agg as m
        -- only to get the platform_name and earliest_offered_timestamp from max
        join {database}.{schema}.title_retail_funnel_metrics as f
            on m.match_id = f.match_id
        left join max_prod.catalog.popcorn_titles as p
            on p.viewable_id = m.match_id
    ),

    fin_meta_data_table as (
        select distinct
            title_name
            , match_id
            , match_id_platform
            , season_number_adj
            , earliest_offered_timestamp
            , platform_name
            , program_type
            , content_category
            , popcorn_ind
            , single_episode_ind
            , in_sequantial_releasing_period
            , at_release_year
            , dayofweek_earliest_date
            , total_hours
            , prod_release_year
            , case when title_age_approx_meta + 1 < title_age_approx_imdb
                then title_age_approx_imdb
                else title_age_approx_meta
                    end as title_age_approx
            , licensor_agg
            , descriptive_genre_desc_agg
            , wm_enterprise_genres_agg
            , navigation_genre_desc_agg
        from fin_meta_data_tmp
        order by match_id, platform_name
    )

    select * from fin_meta_data_table
    order by match_id
) file_format = (type = 'csv' 
        compression = 'NONE' 
        field_optionally_enclosed_by = '"') single = true OVERWRITE = TRUE header = TRUE;
