USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/prelaunch_trailer_feature.csv
FROM (
    with base_info_table as (
            select distinct
                match_id_platform
                , nday_before
                , retail_trailer_view_metric
            from {database}.{schema}.trailer_retail_view_percent
        ),

        viewed_pivot_base as (
            select
                b.match_id_platform
                , concat('DAY_', lpad(to_char(-nday_before),3, 0), '_TRAILER_METRIC_BEFORE') as days_since_first_offered
                , retail_trailer_view_metric
            from base_info_table as b
        ),

        viewed_pivot_table as (
            select
                *
            from viewed_pivot_base
            pivot(mode(retail_trailer_view_metric) for
                days_since_first_offered in (
                          'DAY_000_TRAILER_METRIC_BEFORE'
                        , 'DAY_001_TRAILER_METRIC_BEFORE'
                        , 'DAY_002_TRAILER_METRIC_BEFORE'
                        , 'DAY_003_TRAILER_METRIC_BEFORE'
                        , 'DAY_004_TRAILER_METRIC_BEFORE'
                        , 'DAY_005_TRAILER_METRIC_BEFORE'
                        , 'DAY_006_TRAILER_METRIC_BEFORE'
                        , 'DAY_007_TRAILER_METRIC_BEFORE'
                        , 'DAY_008_TRAILER_METRIC_BEFORE'
                        , 'DAY_009_TRAILER_METRIC_BEFORE'
                        , 'DAY_010_TRAILER_METRIC_BEFORE'
                        , 'DAY_011_TRAILER_METRIC_BEFORE'
                        , 'DAY_012_TRAILER_METRIC_BEFORE'
                        , 'DAY_013_TRAILER_METRIC_BEFORE'
                        , 'DAY_014_TRAILER_METRIC_BEFORE'
                        , 'DAY_015_TRAILER_METRIC_BEFORE'
                        , 'DAY_016_TRAILER_METRIC_BEFORE'
                        , 'DAY_017_TRAILER_METRIC_BEFORE'
                        , 'DAY_018_TRAILER_METRIC_BEFORE'
                        , 'DAY_019_TRAILER_METRIC_BEFORE'
                        , 'DAY_020_TRAILER_METRIC_BEFORE'
                        , 'DAY_021_TRAILER_METRIC_BEFORE'
                        , 'DAY_022_TRAILER_METRIC_BEFORE'
                        , 'DAY_023_TRAILER_METRIC_BEFORE'
                        , 'DAY_024_TRAILER_METRIC_BEFORE'
                        , 'DAY_025_TRAILER_METRIC_BEFORE'
                        , 'DAY_026_TRAILER_METRIC_BEFORE'
                        , 'DAY_027_TRAILER_METRIC_BEFORE'
                        , 'DAY_028_TRAILER_METRIC_BEFORE'
                    )) as p (
                            MATCH_ID_PLATFORM
                            , DAY_000_TRAILER_METRIC_BEFORE
                            , DAY_001_TRAILER_METRIC_BEFORE
                            , DAY_002_TRAILER_METRIC_BEFORE
                            , DAY_003_TRAILER_METRIC_BEFORE
                            , DAY_004_TRAILER_METRIC_BEFORE
                            , DAY_005_TRAILER_METRIC_BEFORE
                            , DAY_006_TRAILER_METRIC_BEFORE
                            , DAY_007_TRAILER_METRIC_BEFORE
                            , DAY_008_TRAILER_METRIC_BEFORE
                            , DAY_009_TRAILER_METRIC_BEFORE
                            , DAY_010_TRAILER_METRIC_BEFORE
                            , DAY_011_TRAILER_METRIC_BEFORE
                            , DAY_012_TRAILER_METRIC_BEFORE
                            , DAY_013_TRAILER_METRIC_BEFORE
                            , DAY_014_TRAILER_METRIC_BEFORE
                            , DAY_015_TRAILER_METRIC_BEFORE
                            , DAY_016_TRAILER_METRIC_BEFORE
                            , DAY_017_TRAILER_METRIC_BEFORE
                            , DAY_018_TRAILER_METRIC_BEFORE
                            , DAY_019_TRAILER_METRIC_BEFORE
                            , DAY_020_TRAILER_METRIC_BEFORE
                            , DAY_021_TRAILER_METRIC_BEFORE
                            , DAY_022_TRAILER_METRIC_BEFORE
                            , DAY_023_TRAILER_METRIC_BEFORE
                            , DAY_024_TRAILER_METRIC_BEFORE
                            , DAY_025_TRAILER_METRIC_BEFORE
                            , DAY_026_TRAILER_METRIC_BEFORE
                            , DAY_027_TRAILER_METRIC_BEFORE
                            , DAY_028_TRAILER_METRIC_BEFORE)
            order by match_id_platform
        ),

        final as (
            select distinct
              match_id_platform
            , case when DAY_000_TRAILER_METRIC_BEFORE is null then -1 else DAY_000_TRAILER_METRIC_BEFORE end as DAY_000_TRAILER_METRIC_BEFORE
            , case when DAY_001_TRAILER_METRIC_BEFORE is null then -1 else DAY_001_TRAILER_METRIC_BEFORE end as DAY_001_TRAILER_METRIC_BEFORE
            , case when DAY_002_TRAILER_METRIC_BEFORE is null then -1 else DAY_002_TRAILER_METRIC_BEFORE end as DAY_002_TRAILER_METRIC_BEFORE
            , case when DAY_003_TRAILER_METRIC_BEFORE is null then -1 else DAY_003_TRAILER_METRIC_BEFORE end as DAY_003_TRAILER_METRIC_BEFORE
            , case when DAY_004_TRAILER_METRIC_BEFORE is null then -1 else DAY_004_TRAILER_METRIC_BEFORE end as DAY_004_TRAILER_METRIC_BEFORE
            , case when DAY_005_TRAILER_METRIC_BEFORE is null then -1 else DAY_005_TRAILER_METRIC_BEFORE end as DAY_005_TRAILER_METRIC_BEFORE
            , case when DAY_006_TRAILER_METRIC_BEFORE is null then -1 else DAY_006_TRAILER_METRIC_BEFORE end as DAY_006_TRAILER_METRIC_BEFORE
            , case when DAY_007_TRAILER_METRIC_BEFORE is null then -1 else DAY_007_TRAILER_METRIC_BEFORE end as DAY_007_TRAILER_METRIC_BEFORE
            , case when DAY_008_TRAILER_METRIC_BEFORE is null then -1 else DAY_008_TRAILER_METRIC_BEFORE end as DAY_008_TRAILER_METRIC_BEFORE
            , case when DAY_009_TRAILER_METRIC_BEFORE is null then -1 else DAY_009_TRAILER_METRIC_BEFORE end as DAY_009_TRAILER_METRIC_BEFORE
            , case when DAY_010_TRAILER_METRIC_BEFORE is null then -1 else DAY_010_TRAILER_METRIC_BEFORE end as DAY_010_TRAILER_METRIC_BEFORE
            , case when DAY_011_TRAILER_METRIC_BEFORE is null then -1 else DAY_011_TRAILER_METRIC_BEFORE end as DAY_011_TRAILER_METRIC_BEFORE
            , case when DAY_012_TRAILER_METRIC_BEFORE is null then -1 else DAY_012_TRAILER_METRIC_BEFORE end as DAY_012_TRAILER_METRIC_BEFORE
            , case when DAY_013_TRAILER_METRIC_BEFORE is null then -1 else DAY_013_TRAILER_METRIC_BEFORE end as DAY_013_TRAILER_METRIC_BEFORE
            , case when DAY_014_TRAILER_METRIC_BEFORE is null then -1 else DAY_014_TRAILER_METRIC_BEFORE end as DAY_014_TRAILER_METRIC_BEFORE
            , case when DAY_015_TRAILER_METRIC_BEFORE is null then -1 else DAY_015_TRAILER_METRIC_BEFORE end as DAY_015_TRAILER_METRIC_BEFORE
            , case when DAY_016_TRAILER_METRIC_BEFORE is null then -1 else DAY_016_TRAILER_METRIC_BEFORE end as DAY_016_TRAILER_METRIC_BEFORE
            , case when DAY_017_TRAILER_METRIC_BEFORE is null then -1 else DAY_017_TRAILER_METRIC_BEFORE end as DAY_017_TRAILER_METRIC_BEFORE
            , case when DAY_018_TRAILER_METRIC_BEFORE is null then -1 else DAY_018_TRAILER_METRIC_BEFORE end as DAY_018_TRAILER_METRIC_BEFORE
            , case when DAY_019_TRAILER_METRIC_BEFORE is null then -1 else DAY_019_TRAILER_METRIC_BEFORE end as DAY_019_TRAILER_METRIC_BEFORE
            , case when DAY_020_TRAILER_METRIC_BEFORE is null then -1 else DAY_020_TRAILER_METRIC_BEFORE end as DAY_020_TRAILER_METRIC_BEFORE
            , case when DAY_021_TRAILER_METRIC_BEFORE is null then -1 else DAY_021_TRAILER_METRIC_BEFORE end as DAY_021_TRAILER_METRIC_BEFORE
            , case when DAY_022_TRAILER_METRIC_BEFORE is null then -1 else DAY_022_TRAILER_METRIC_BEFORE end as DAY_022_TRAILER_METRIC_BEFORE
            , case when DAY_023_TRAILER_METRIC_BEFORE is null then -1 else DAY_023_TRAILER_METRIC_BEFORE end as DAY_023_TRAILER_METRIC_BEFORE
            , case when DAY_024_TRAILER_METRIC_BEFORE is null then -1 else DAY_024_TRAILER_METRIC_BEFORE end as DAY_024_TRAILER_METRIC_BEFORE
            , case when DAY_025_TRAILER_METRIC_BEFORE is null then -1 else DAY_025_TRAILER_METRIC_BEFORE end as DAY_025_TRAILER_METRIC_BEFORE
            , case when DAY_026_TRAILER_METRIC_BEFORE is null then -1 else DAY_026_TRAILER_METRIC_BEFORE end as DAY_026_TRAILER_METRIC_BEFORE
            , case when DAY_027_TRAILER_METRIC_BEFORE is null then -1 else DAY_027_TRAILER_METRIC_BEFORE end as DAY_027_TRAILER_METRIC_BEFORE
            , case when DAY_028_TRAILER_METRIC_BEFORE is null then -1 else DAY_028_TRAILER_METRIC_BEFORE end as DAY_028_TRAILER_METRIC_BEFORE
        from viewed_pivot_table as v
        right join {database}.{schema}.title_retail_funnel_metrics as f
            on v.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                                                          else 1 end, '-', f.match_id)
        where match_id_platform is not null
        )

        select
            f.*
        from final as f
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;