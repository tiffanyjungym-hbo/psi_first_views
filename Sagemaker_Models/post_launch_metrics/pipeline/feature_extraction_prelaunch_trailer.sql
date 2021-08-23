USE SCHEMA workspace;

COPY INTO {stage}/input_prelaunch_percent_view/prelaunch_trailer_feature.csv
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
                , concat('DAY_BEFORE_', lpad(to_char(-nday_before),3, 0), '_TRAILER_PERCENT_VIEW') as days_since_first_offered
                , retail_trailer_view_metric
            from base_info_table as b
        ),

        viewed_pivot_table as (
            select
                *
            from viewed_pivot_base
            pivot(mode(retail_trailer_view_metric) for
                days_since_first_offered in (
                          'DAY_BEFORE_000_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_001_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_002_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_003_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_004_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_005_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_006_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_007_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_008_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_009_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_010_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_011_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_012_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_013_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_014_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_015_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_016_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_017_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_018_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_019_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_020_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_021_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_022_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_023_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_024_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_025_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_026_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_027_TRAILER_PERCENT_VIEW'
                        , 'DAY_BEFORE_028_TRAILER_PERCENT_VIEW'
                    )) as p (
                            MATCH_ID_PLATFORM
                            , DAY_BEFORE_000_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_001_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_002_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_003_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_004_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_005_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_006_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_007_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_008_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_009_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_010_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_011_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_012_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_013_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_014_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_015_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_016_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_017_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_018_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_019_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_020_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_021_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_022_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_023_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_024_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_025_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_026_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_027_TRAILER_PERCENT_VIEW
                            , DAY_BEFORE_028_TRAILER_PERCENT_VIEW)
            order by match_id_platform
        ),

        final as (
            select distinct
              match_id_platform
            , case when DAY_BEFORE_000_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_000_TRAILER_PERCENT_VIEW end as DAY_BEFORE_000_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_001_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_001_TRAILER_PERCENT_VIEW end as DAY_BEFORE_001_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_002_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_002_TRAILER_PERCENT_VIEW end as DAY_BEFORE_002_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_003_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_003_TRAILER_PERCENT_VIEW end as DAY_BEFORE_003_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_004_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_004_TRAILER_PERCENT_VIEW end as DAY_BEFORE_004_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_005_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_005_TRAILER_PERCENT_VIEW end as DAY_BEFORE_005_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_006_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_006_TRAILER_PERCENT_VIEW end as DAY_BEFORE_006_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_007_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_007_TRAILER_PERCENT_VIEW end as DAY_BEFORE_007_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_008_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_008_TRAILER_PERCENT_VIEW end as DAY_BEFORE_008_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_009_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_009_TRAILER_PERCENT_VIEW end as DAY_BEFORE_009_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_010_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_010_TRAILER_PERCENT_VIEW end as DAY_BEFORE_010_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_011_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_011_TRAILER_PERCENT_VIEW end as DAY_BEFORE_011_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_012_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_012_TRAILER_PERCENT_VIEW end as DAY_BEFORE_012_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_013_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_013_TRAILER_PERCENT_VIEW end as DAY_BEFORE_013_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_014_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_014_TRAILER_PERCENT_VIEW end as DAY_BEFORE_014_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_015_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_015_TRAILER_PERCENT_VIEW end as DAY_BEFORE_015_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_016_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_016_TRAILER_PERCENT_VIEW end as DAY_BEFORE_016_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_017_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_017_TRAILER_PERCENT_VIEW end as DAY_BEFORE_017_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_018_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_018_TRAILER_PERCENT_VIEW end as DAY_BEFORE_018_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_019_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_019_TRAILER_PERCENT_VIEW end as DAY_BEFORE_019_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_020_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_020_TRAILER_PERCENT_VIEW end as DAY_BEFORE_020_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_021_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_021_TRAILER_PERCENT_VIEW end as DAY_BEFORE_021_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_022_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_022_TRAILER_PERCENT_VIEW end as DAY_BEFORE_022_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_023_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_023_TRAILER_PERCENT_VIEW end as DAY_BEFORE_023_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_024_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_024_TRAILER_PERCENT_VIEW end as DAY_BEFORE_024_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_025_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_025_TRAILER_PERCENT_VIEW end as DAY_BEFORE_025_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_026_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_026_TRAILER_PERCENT_VIEW end as DAY_BEFORE_026_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_027_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_027_TRAILER_PERCENT_VIEW end as DAY_BEFORE_027_TRAILER_PERCENT_VIEW
            , case when DAY_BEFORE_028_TRAILER_PERCENT_VIEW is null then -1 else DAY_BEFORE_028_TRAILER_PERCENT_VIEW end as DAY_BEFORE_028_TRAILER_PERCENT_VIEW
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