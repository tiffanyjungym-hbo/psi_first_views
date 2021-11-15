USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/wiki_view_post_feature.csv
FROM (

    with tmp as (
        select
            title_name
            , match_id_platform
            , days_since_first_offered
            , sum(total_page_views) over
                (partition by match_id_platform
                    order by days_since_first_offered asc) as wiki_befored28_total
        from {database}.{schema}.title_wikipedia_page_view as v
        where v.days_since_first_offered between 1 and 27
        order by match_id_platform,days_since_first_offered
    ),

    pivot_base as (
        select
            match_id_platform
            , concat('DAY', lpad(to_char(days_since_first_offered),3, 0), '_WIKI_VIEW') as days_since_first_offered
            , ifnull(wiki_befored28_total, -1) as wiki_befored28_total
        from tmp
    ),

    viewed_pivot_table as (
        select
            *
        from pivot_base
        pivot(mode(wiki_befored28_total) for
            days_since_first_offered in (
                      'DAY001_WIKI_VIEW'
                    , 'DAY002_WIKI_VIEW'
                    , 'DAY003_WIKI_VIEW'
                    , 'DAY004_WIKI_VIEW'
                    , 'DAY005_WIKI_VIEW'
                    , 'DAY006_WIKI_VIEW'
                    , 'DAY007_WIKI_VIEW'
                    , 'DAY008_WIKI_VIEW'
                    , 'DAY009_WIKI_VIEW'
                    , 'DAY010_WIKI_VIEW'
                    , 'DAY011_WIKI_VIEW'
                    , 'DAY012_WIKI_VIEW'
                    , 'DAY013_WIKI_VIEW'
                    , 'DAY014_WIKI_VIEW'
                    , 'DAY015_WIKI_VIEW'
                    , 'DAY016_WIKI_VIEW'
                    , 'DAY017_WIKI_VIEW'
                    , 'DAY018_WIKI_VIEW'
                    , 'DAY019_WIKI_VIEW'
                    , 'DAY020_WIKI_VIEW'
                    , 'DAY021_WIKI_VIEW'
                    , 'DAY022_WIKI_VIEW'
                    , 'DAY023_WIKI_VIEW'
                    , 'DAY024_WIKI_VIEW'
                    , 'DAY025_WIKI_VIEW'
                    , 'DAY026_WIKI_VIEW'
                    , 'DAY027_WIKI_VIEW'
                )) as p (
                        MATCH_ID_PLATFORM
                        , DAY001_WIKI_VIEW
                        , DAY002_WIKI_VIEW
                        , DAY003_WIKI_VIEW
                        , DAY004_WIKI_VIEW
                        , DAY005_WIKI_VIEW
                        , DAY006_WIKI_VIEW
                        , DAY007_WIKI_VIEW
                        , DAY008_WIKI_VIEW
                        , DAY009_WIKI_VIEW
                        , DAY010_WIKI_VIEW
                        , DAY011_WIKI_VIEW
                        , DAY012_WIKI_VIEW
                        , DAY013_WIKI_VIEW
                        , DAY014_WIKI_VIEW
                        , DAY015_WIKI_VIEW
                        , DAY016_WIKI_VIEW
                        , DAY017_WIKI_VIEW
                        , DAY018_WIKI_VIEW
                        , DAY019_WIKI_VIEW
                        , DAY020_WIKI_VIEW
                        , DAY021_WIKI_VIEW
                        , DAY022_WIKI_VIEW
                        , DAY023_WIKI_VIEW
                        , DAY024_WIKI_VIEW
                        , DAY025_WIKI_VIEW
                        , DAY026_WIKI_VIEW
                        , DAY027_WIKI_VIEW
                        )
        order by match_id_platform
    ),

    final as (
        select distinct
        concat(case when f.platform_name = 'hboNow' then 0 else 1 end, '-', f.match_id) as match_id_platform
        , case when DAY001_WIKI_VIEW is null then -1 else DAY001_WIKI_VIEW end as DAY001_WIKI_VIEW
        , case when DAY002_WIKI_VIEW is null then -1 else DAY002_WIKI_VIEW end as DAY002_WIKI_VIEW
        , case when DAY003_WIKI_VIEW is null then -1 else DAY003_WIKI_VIEW end as DAY003_WIKI_VIEW
        , case when DAY004_WIKI_VIEW is null then -1 else DAY004_WIKI_VIEW end as DAY004_WIKI_VIEW
        , case when DAY005_WIKI_VIEW is null then -1 else DAY005_WIKI_VIEW end as DAY005_WIKI_VIEW
        , case when DAY006_WIKI_VIEW is null then -1 else DAY006_WIKI_VIEW end as DAY006_WIKI_VIEW
        , case when DAY007_WIKI_VIEW is null then -1 else DAY007_WIKI_VIEW end as DAY007_WIKI_VIEW
        , case when DAY008_WIKI_VIEW is null then -1 else DAY008_WIKI_VIEW end as DAY008_WIKI_VIEW
        , case when DAY009_WIKI_VIEW is null then -1 else DAY009_WIKI_VIEW end as DAY009_WIKI_VIEW
        , case when DAY010_WIKI_VIEW is null then -1 else DAY010_WIKI_VIEW end as DAY010_WIKI_VIEW
        , case when DAY011_WIKI_VIEW is null then -1 else DAY011_WIKI_VIEW end as DAY011_WIKI_VIEW
        , case when DAY012_WIKI_VIEW is null then -1 else DAY012_WIKI_VIEW end as DAY012_WIKI_VIEW
        , case when DAY013_WIKI_VIEW is null then -1 else DAY013_WIKI_VIEW end as DAY013_WIKI_VIEW
        , case when DAY014_WIKI_VIEW is null then -1 else DAY014_WIKI_VIEW end as DAY014_WIKI_VIEW
        , case when DAY015_WIKI_VIEW is null then -1 else DAY015_WIKI_VIEW end as DAY015_WIKI_VIEW
        , case when DAY016_WIKI_VIEW is null then -1 else DAY016_WIKI_VIEW end as DAY016_WIKI_VIEW
        , case when DAY017_WIKI_VIEW is null then -1 else DAY017_WIKI_VIEW end as DAY017_WIKI_VIEW
        , case when DAY018_WIKI_VIEW is null then -1 else DAY018_WIKI_VIEW end as DAY018_WIKI_VIEW
        , case when DAY019_WIKI_VIEW is null then -1 else DAY019_WIKI_VIEW end as DAY019_WIKI_VIEW
        , case when DAY020_WIKI_VIEW is null then -1 else DAY020_WIKI_VIEW end as DAY020_WIKI_VIEW
        , case when DAY021_WIKI_VIEW is null then -1 else DAY021_WIKI_VIEW end as DAY021_WIKI_VIEW
        , case when DAY022_WIKI_VIEW is null then -1 else DAY022_WIKI_VIEW end as DAY022_WIKI_VIEW
        , case when DAY023_WIKI_VIEW is null then -1 else DAY023_WIKI_VIEW end as DAY023_WIKI_VIEW
        , case when DAY024_WIKI_VIEW is null then -1 else DAY024_WIKI_VIEW end as DAY024_WIKI_VIEW
        , case when DAY025_WIKI_VIEW is null then -1 else DAY025_WIKI_VIEW end as DAY025_WIKI_VIEW
        , case when DAY026_WIKI_VIEW is null then -1 else DAY026_WIKI_VIEW end as DAY026_WIKI_VIEW
        , case when DAY027_WIKI_VIEW is null then -1 else DAY027_WIKI_VIEW end as DAY027_WIKI_VIEW
    from viewed_pivot_table as v
    right join {database}.{schema}.title_retail_funnel_metrics as f
        on v.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                                            else 1 end, '-', f.match_id)
    )
    select
        f.*
    from final as f

) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



