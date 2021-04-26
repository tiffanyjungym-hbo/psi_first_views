USE SCHEMA workspace;

COPY INTO {stage}/sub_total_feature.csv
FROM (
    with base_info_table as (
        select distinct
            concat(case when platform_name = 'hboNow' then 0
                    else 1 end, '-', match_id) as match_id_platform
            , days_since_first_offered
            , datediff(day, earliest_offered_timestamp, last_update_timestamp) as days_since_offered
            , total_retail_sub_count
        from {database}.{schema}.title_retail_funnel_metrics
    ),

    min_days_since_offered_table as (
        select
            match_id_platform
            , max(days_since_offered) as min_days_since_offered
        from base_info_table
        group by 1

    ),

    viewed_pivot_base as (
        select
            b.match_id_platform
            , min_days_since_offered
            , concat('DAY', lpad(to_char(days_since_first_offered),3, 0), '_SUB_COUNT') as days_since_first_offered
            , total_retail_sub_count
        from base_info_table as b
        join min_days_since_offered_table as md
            on md.match_id_platform = b.match_id_platform
    ),

    viewed_pivot_table as (
        select
            *
        from viewed_pivot_base
        pivot(mode(retail_viewed_count_percent) for
            days_since_first_offered in (
                    'DAY001_SUB_COUNT'
                    , 'DAY002_SUB_COUNT'
                    , 'DAY003_SUB_COUNT'
                    , 'DAY004_SUB_COUNT'
                    , 'DAY005_SUB_COUNT'
                    , 'DAY006_SUB_COUNT'
                    , 'DAY007_SUB_COUNT'
                    , 'DAY008_SUB_COUNT'
                    , 'DAY009_SUB_COUNT'
                    , 'DAY010_SUB_COUNT'
                    , 'DAY011_SUB_COUNT'
                    , 'DAY012_SUB_COUNT'
                    , 'DAY013_SUB_COUNT'
                    , 'DAY014_SUB_COUNT'
                    , 'DAY015_SUB_COUNT'
                    , 'DAY016_SUB_COUNT'
                    , 'DAY017_SUB_COUNT'
                    , 'DAY018_SUB_COUNT'
                    , 'DAY019_SUB_COUNT'
                    , 'DAY020_SUB_COUNT'
                    , 'DAY021_SUB_COUNT'
                    , 'DAY022_SUB_COUNT'
                    , 'DAY023_SUB_COUNT'
                    , 'DAY024_SUB_COUNT'
                    , 'DAY025_SUB_COUNT'
                    , 'DAY026_SUB_COUNT'
                    , 'DAY027_SUB_COUNT'
                    , 'DAY028_SUB_COUNT'
                ))
        as viewed_percent
        order by match_id_platform
    )

    select
        v.*
    from viewed_pivot_table as v
    order by match_id_platform
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



