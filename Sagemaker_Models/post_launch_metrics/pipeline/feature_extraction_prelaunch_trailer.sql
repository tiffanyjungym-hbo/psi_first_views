USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/prelaunch_trailer_feature.csv
FROM (
    with base_info_table as (
            select distinct
                match_id_platform
                , nday_before
                , retail_trailer_view_metric
            from max_dev.workspace.trailer_retail_view_percent_d28
    ),

    viewed_pivot_base as (
        select
            b.match_id_platform
            , concat('DAY', lpad(to_char(-nday_before),3, 0), '_TRAILER_METRIC_BEFORE') as days_since_first_offered
            , retail_trailer_view_metric
        from base_info_table as b
    ),

    viewed_pivot_table as (
        select
            *
        from viewed_pivot_base
        pivot(mode(retail_trailer_view_metric) for
            days_since_first_offered in (
                    'DAY000_TRAILER_METRIC_BEFORE'
                    , 'DAY001_TRAILER_METRIC_BEFORE'
                    , 'DAY002_TRAILER_METRIC_BEFORE'
                    , 'DAY003_TRAILER_METRIC_BEFORE'
                    , 'DAY004_TRAILER_METRIC_BEFORE'
                    , 'DAY005_TRAILER_METRIC_BEFORE'
                    , 'DAY006_TRAILER_METRIC_BEFORE'
                    , 'DAY007_TRAILER_METRIC_BEFORE'
                    , 'DAY008_TRAILER_METRIC_BEFORE'
                    , 'DAY009_TRAILER_METRIC_BEFORE'
                    , 'DAY010_TRAILER_METRIC_BEFORE'
                    , 'DAY011_TRAILER_METRIC_BEFORE'
                    , 'DAY012_TRAILER_METRIC_BEFORE'
                    , 'DAY013_TRAILER_METRIC_BEFORE'
                    , 'DAY014_TRAILER_METRIC_BEFORE'
                    , 'DAY015_TRAILER_METRIC_BEFORE'
                    , 'DAY016_TRAILER_METRIC_BEFORE'
                    , 'DAY017_TRAILER_METRIC_BEFORE'
                    , 'DAY018_TRAILER_METRIC_BEFORE'
                    , 'DAY019_TRAILER_METRIC_BEFORE'
                    , 'DAY020_TRAILER_METRIC_BEFORE'
                    , 'DAY021_TRAILER_METRIC_BEFORE'
                    , 'DAY022_TRAILER_METRIC_BEFORE'
                    , 'DAY023_TRAILER_METRIC_BEFORE'
                    , 'DAY024_TRAILER_METRIC_BEFORE'
                    , 'DAY025_TRAILER_METRIC_BEFORE'
                    , 'DAY026_TRAILER_METRIC_BEFORE'
                    , 'DAY027_TRAILER_METRIC_BEFORE'
                )) as p (
                        MATCH_ID_PLATFORM
                        , DAY000_TRAILER_METRIC_D28
                        , DAY001_TRAILER_METRIC_D28
                        , DAY002_TRAILER_METRIC_D28
                        , DAY003_TRAILER_METRIC_D28
                        , DAY004_TRAILER_METRIC_D28
                        , DAY005_TRAILER_METRIC_D28
                        , DAY006_TRAILER_METRIC_D28
                        , DAY007_TRAILER_METRIC_D28
                        , DAY008_TRAILER_METRIC_D28
                        , DAY009_TRAILER_METRIC_D28
                        , DAY010_TRAILER_METRIC_D28
                        , DAY011_TRAILER_METRIC_D28
                        , DAY012_TRAILER_METRIC_D28
                        , DAY013_TRAILER_METRIC_D28
                        , DAY014_TRAILER_METRIC_D28
                        , DAY015_TRAILER_METRIC_D28
                        , DAY016_TRAILER_METRIC_D28
                        , DAY017_TRAILER_METRIC_D28
                        , DAY018_TRAILER_METRIC_D28
                        , DAY019_TRAILER_METRIC_D28
                        , DAY020_TRAILER_METRIC_D28
                        , DAY021_TRAILER_METRIC_D28
                        , DAY022_TRAILER_METRIC_D28
                        , DAY023_TRAILER_METRIC_D28
                        , DAY024_TRAILER_METRIC_D28
                        , DAY025_TRAILER_METRIC_D28
                        , DAY026_TRAILER_METRIC_D28
                        , DAY027_TRAILER_METRIC_D28)
        order by match_id_platform
    ),

    final as (
        select distinct
        concat(case when f.platform_name = 'hboNow' then 0 else 1 end, '-', f.match_id) as match_id_platform
        , case when DAY000_TRAILER_METRIC_D28 is null then -1 else DAY000_TRAILER_METRIC_D28 end as DAY000_TRAILER_METRIC_D28
        , case when DAY001_TRAILER_METRIC_D28 is null then -1 else DAY001_TRAILER_METRIC_D28 end as DAY001_TRAILER_METRIC_D28
        , case when DAY002_TRAILER_METRIC_D28 is null then -1 else DAY002_TRAILER_METRIC_D28 end as DAY002_TRAILER_METRIC_D28
        , case when DAY003_TRAILER_METRIC_D28 is null then -1 else DAY003_TRAILER_METRIC_D28 end as DAY003_TRAILER_METRIC_D28
        , case when DAY004_TRAILER_METRIC_D28 is null then -1 else DAY004_TRAILER_METRIC_D28 end as DAY004_TRAILER_METRIC_D28
        , case when DAY005_TRAILER_METRIC_D28 is null then -1 else DAY005_TRAILER_METRIC_D28 end as DAY005_TRAILER_METRIC_D28
        , case when DAY006_TRAILER_METRIC_D28 is null then -1 else DAY006_TRAILER_METRIC_D28 end as DAY006_TRAILER_METRIC_D28
        , case when DAY007_TRAILER_METRIC_D28 is null then -1 else DAY007_TRAILER_METRIC_D28 end as DAY007_TRAILER_METRIC_D28
        , case when DAY008_TRAILER_METRIC_D28 is null then -1 else DAY008_TRAILER_METRIC_D28 end as DAY008_TRAILER_METRIC_D28
        , case when DAY009_TRAILER_METRIC_D28 is null then -1 else DAY009_TRAILER_METRIC_D28 end as DAY009_TRAILER_METRIC_D28
        , case when DAY010_TRAILER_METRIC_D28 is null then -1 else DAY010_TRAILER_METRIC_D28 end as DAY010_TRAILER_METRIC_D28
        , case when DAY011_TRAILER_METRIC_D28 is null then -1 else DAY011_TRAILER_METRIC_D28 end as DAY011_TRAILER_METRIC_D28
        , case when DAY012_TRAILER_METRIC_D28 is null then -1 else DAY012_TRAILER_METRIC_D28 end as DAY012_TRAILER_METRIC_D28
        , case when DAY013_TRAILER_METRIC_D28 is null then -1 else DAY013_TRAILER_METRIC_D28 end as DAY013_TRAILER_METRIC_D28
        , case when DAY014_TRAILER_METRIC_D28 is null then -1 else DAY014_TRAILER_METRIC_D28 end as DAY014_TRAILER_METRIC_D28
        , case when DAY015_TRAILER_METRIC_D28 is null then -1 else DAY015_TRAILER_METRIC_D28 end as DAY015_TRAILER_METRIC_D28
        , case when DAY016_TRAILER_METRIC_D28 is null then -1 else DAY016_TRAILER_METRIC_D28 end as DAY016_TRAILER_METRIC_D28
        , case when DAY017_TRAILER_METRIC_D28 is null then -1 else DAY017_TRAILER_METRIC_D28 end as DAY017_TRAILER_METRIC_D28
        , case when DAY018_TRAILER_METRIC_D28 is null then -1 else DAY018_TRAILER_METRIC_D28 end as DAY018_TRAILER_METRIC_D28
        , case when DAY019_TRAILER_METRIC_D28 is null then -1 else DAY019_TRAILER_METRIC_D28 end as DAY019_TRAILER_METRIC_D28
        , case when DAY020_TRAILER_METRIC_D28 is null then -1 else DAY020_TRAILER_METRIC_D28 end as DAY020_TRAILER_METRIC_D28
        , case when DAY021_TRAILER_METRIC_D28 is null then -1 else DAY021_TRAILER_METRIC_D28 end as DAY021_TRAILER_METRIC_D28
        , case when DAY022_TRAILER_METRIC_D28 is null then -1 else DAY022_TRAILER_METRIC_D28 end as DAY022_TRAILER_METRIC_D28
        , case when DAY023_TRAILER_METRIC_D28 is null then -1 else DAY023_TRAILER_METRIC_D28 end as DAY023_TRAILER_METRIC_D28
        , case when DAY024_TRAILER_METRIC_D28 is null then -1 else DAY024_TRAILER_METRIC_D28 end as DAY024_TRAILER_METRIC_D28
        , case when DAY025_TRAILER_METRIC_D28 is null then -1 else DAY025_TRAILER_METRIC_D28 end as DAY025_TRAILER_METRIC_D28
        , case when DAY026_TRAILER_METRIC_D28 is null then -1 else DAY026_TRAILER_METRIC_D28 end as DAY026_TRAILER_METRIC_D28
        , case when DAY027_TRAILER_METRIC_D28 is null then -1 else DAY027_TRAILER_METRIC_D28 end as DAY027_TRAILER_METRIC_D28
    from viewed_pivot_table as v
    right join max_dev.workspace.title_retail_funnel_metrics as f
        on v.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                                            else 1 end, '-', f.match_id)
    )

    select
        f.*
    from final as f
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;