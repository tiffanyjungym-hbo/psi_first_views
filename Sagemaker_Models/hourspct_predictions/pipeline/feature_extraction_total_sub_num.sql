USE SCHEMA workspace;

COPY INTO {stage}/hourspct/sub_total_feature.csv
FROM (
    with base_info_table as (
        select distinct
            match_id
            , days_since_first_offered
            , datediff(day, earliest_offered_timestamp, last_update_timestamp) as days_since_offered
            , total_retail_sub_count
        from {database}.{schema}.title_retail_funnel_metrics
    ),

    viewed_pivot_base as (
        select distinct
            b.match_id
            , concat('DAY', lpad(to_char(days_since_first_offered),3, 0), '_SUB_COUNT') as days_since_first_offered
            , total_retail_sub_count
        from base_info_table as b
    ),

    viewed_pivot_table as (
        select distinct
            * 
        from viewed_pivot_base
        pivot(mode(total_retail_sub_count) for
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
        order by match_id
    )

    select
        v.*
    from viewed_pivot_table as v
    order by match_id
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



