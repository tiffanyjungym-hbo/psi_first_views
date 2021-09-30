USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/funnel_metric_feature.csv
FROM (

    with base_info_table as (
        select distinct
            concat(case when platform_name = 'hboNow' then 0
                    else 1 end, '-', match_id) as match_id_platform
            , days_since_first_offered
            , retail_viewed_count_percent
        from {database}.{schema}.title_retail_funnel_metrics
        group by 1,2,3
    ),

    min_days_since_offered_table as (
        select
            match_id_platform
            , max(days_since_first_offered) as min_days_since_offered
        from base_info_table
        group by 1

    ),

    viewed_pivot_base as (
        select
            b.match_id_platform
            , min_days_since_offered
            , concat('DAY', lpad(to_char(days_since_first_offered),3, 0), '_PERCENT_VIEWED') as days_since_first_offered
            , retail_viewed_count_percent
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
                    'DAY001_PERCENT_VIEWED'
                    , 'DAY002_PERCENT_VIEWED'
                    , 'DAY003_PERCENT_VIEWED'
                    , 'DAY004_PERCENT_VIEWED'
                    , 'DAY005_PERCENT_VIEWED'
                    , 'DAY006_PERCENT_VIEWED'
                    , 'DAY007_PERCENT_VIEWED'
                    , 'DAY008_PERCENT_VIEWED'
                    , 'DAY009_PERCENT_VIEWED'
                    , 'DAY010_PERCENT_VIEWED'
                    , 'DAY011_PERCENT_VIEWED'
                    , 'DAY012_PERCENT_VIEWED'
                    , 'DAY013_PERCENT_VIEWED'
                    , 'DAY014_PERCENT_VIEWED'
                    , 'DAY015_PERCENT_VIEWED'
                    , 'DAY016_PERCENT_VIEWED'
                    , 'DAY017_PERCENT_VIEWED'
                    , 'DAY018_PERCENT_VIEWED'
                    , 'DAY019_PERCENT_VIEWED'
                    , 'DAY020_PERCENT_VIEWED'
                    , 'DAY021_PERCENT_VIEWED'
                    , 'DAY022_PERCENT_VIEWED'
                    , 'DAY023_PERCENT_VIEWED'
                    , 'DAY024_PERCENT_VIEWED'
                    , 'DAY025_PERCENT_VIEWED'
                    , 'DAY026_PERCENT_VIEWED'
                    , 'DAY027_PERCENT_VIEWED'
                    , 'DAY028_PERCENT_VIEWED'
                )) as p ( 
                      MATCH_ID_PLATFORM
                    , MAX_DAYS_SINCE_FIRST_OFFERED
                    , DAY001_PERCENT_VIEWED
                    , DAY002_PERCENT_VIEWED
                    , DAY003_PERCENT_VIEWED
                    , DAY004_PERCENT_VIEWED
                    , DAY005_PERCENT_VIEWED
                    , DAY006_PERCENT_VIEWED
                    , DAY007_PERCENT_VIEWED
                    , DAY008_PERCENT_VIEWED
                    , DAY009_PERCENT_VIEWED
                    , DAY010_PERCENT_VIEWED
                    , DAY011_PERCENT_VIEWED
                    , DAY012_PERCENT_VIEWED
                    , DAY013_PERCENT_VIEWED
                    , DAY014_PERCENT_VIEWED
                    , DAY015_PERCENT_VIEWED
                    , DAY016_PERCENT_VIEWED
                    , DAY017_PERCENT_VIEWED
                    , DAY018_PERCENT_VIEWED
                    , DAY019_PERCENT_VIEWED
                    , DAY020_PERCENT_VIEWED
                    , DAY021_PERCENT_VIEWED
                    , DAY022_PERCENT_VIEWED
                    , DAY023_PERCENT_VIEWED
                    , DAY024_PERCENT_VIEWED
                    , DAY025_PERCENT_VIEWED
                    , DAY026_PERCENT_VIEWED
                    , DAY027_PERCENT_VIEWED
                    , DAY028_PERCENT_VIEWED
                )
        order by match_id_platform
    )

    select
        v.*
    from viewed_pivot_table as v
    order by match_id_platform

) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



