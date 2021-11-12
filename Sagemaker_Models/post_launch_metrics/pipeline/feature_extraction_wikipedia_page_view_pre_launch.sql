USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/wiki_view_pre_feature.csv
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
    where v.days_since_first_offered between -27 and 0
    order by match_id_platform,days_since_first_offered
    ),

    pivot_base as (select
          match_id_platform
        , concat('DAY', lpad(to_char(-days_since_first_offered),3, 0), '_WIKI_D28') as days_since_first_offered
        , ifnull(wiki_befored28_total, -1) as wiki_befored28_total
    from tmp
    ),

    viewed_pivot_table as (
        select
            *
        from pivot_base
        pivot(mode(wiki_befored28_total) for
            days_since_first_offered in (
                      'DAY027_WIKI_D28'
                    , 'DAY026_WIKI_D28'
                    , 'DAY025_WIKI_D28'
                    , 'DAY024_WIKI_D28'
                    , 'DAY023_WIKI_D28'
                    , 'DAY022_WIKI_D28'
                    , 'DAY021_WIKI_D28'
                    , 'DAY020_WIKI_D28'
                    , 'DAY019_WIKI_D28'
                    , 'DAY018_WIKI_D28'
                    , 'DAY017_WIKI_D28'
                    , 'DAY016_WIKI_D28'
                    , 'DAY015_WIKI_D28'
                    , 'DAY014_WIKI_D28'
                    , 'DAY013_WIKI_D28'
                    , 'DAY012_WIKI_D28'
                    , 'DAY011_WIKI_D28'
                    , 'DAY010_WIKI_D28'
                    , 'DAY009_WIKI_D28'
                    , 'DAY008_WIKI_D28'
                    , 'DAY007_WIKI_D28'
                    , 'DAY006_WIKI_D28'
                    , 'DAY005_WIKI_D28'
                    , 'DAY004_WIKI_D28'
                    , 'DAY003_WIKI_D28'
                    , 'DAY002_WIKI_D28'
                    , 'DAY001_WIKI_D28'
                    , 'DAY000_WIKI_D28'
                )) as p (
                        MATCH_ID_PLATFORM
                        , DAY027_WIKI_D28
                        , DAY026_WIKI_D28
                        , DAY025_WIKI_D28
                        , DAY024_WIKI_D28
                        , DAY023_WIKI_D28
                        , DAY022_WIKI_D28
                        , DAY021_WIKI_D28
                        , DAY020_WIKI_D28
                        , DAY019_WIKI_D28
                        , DAY018_WIKI_D28
                        , DAY017_WIKI_D28
                        , DAY016_WIKI_D28
                        , DAY015_WIKI_D28
                        , DAY014_WIKI_D28
                        , DAY013_WIKI_D28
                        , DAY012_WIKI_D28
                        , DAY011_WIKI_D28
                        , DAY010_WIKI_D28
                        , DAY009_WIKI_D28
                        , DAY008_WIKI_D28
                        , DAY007_WIKI_D28
                        , DAY006_WIKI_D28
                        , DAY005_WIKI_D28
                        , DAY004_WIKI_D28
                        , DAY003_WIKI_D28
                        , DAY002_WIKI_D28
                        , DAY001_WIKI_D28
                        , DAY000_WIKI_D28)
        order by match_id_platform
    ),

    final as (
        select distinct
        concat(case when f.platform_name = 'hboNow' then 0 else 1 end, '-', f.match_id) as match_id_platform
        , case when DAY027_WIKI_D28 is null then -1 else DAY027_WIKI_D28 end as DAY027_WIKI_D28
        , case when DAY026_WIKI_D28 is null then -1 else DAY026_WIKI_D28 end as DAY026_WIKI_D28
        , case when DAY025_WIKI_D28 is null then -1 else DAY025_WIKI_D28 end as DAY025_WIKI_D28
        , case when DAY024_WIKI_D28 is null then -1 else DAY024_WIKI_D28 end as DAY024_WIKI_D28
        , case when DAY023_WIKI_D28 is null then -1 else DAY023_WIKI_D28 end as DAY023_WIKI_D28
        , case when DAY022_WIKI_D28 is null then -1 else DAY022_WIKI_D28 end as DAY022_WIKI_D28
        , case when DAY021_WIKI_D28 is null then -1 else DAY021_WIKI_D28 end as DAY021_WIKI_D28
        , case when DAY020_WIKI_D28 is null then -1 else DAY020_WIKI_D28 end as DAY020_WIKI_D28
        , case when DAY019_WIKI_D28 is null then -1 else DAY019_WIKI_D28 end as DAY019_WIKI_D28
        , case when DAY018_WIKI_D28 is null then -1 else DAY018_WIKI_D28 end as DAY018_WIKI_D28
        , case when DAY017_WIKI_D28 is null then -1 else DAY017_WIKI_D28 end as DAY017_WIKI_D28
        , case when DAY016_WIKI_D28 is null then -1 else DAY016_WIKI_D28 end as DAY016_WIKI_D28
        , case when DAY015_WIKI_D28 is null then -1 else DAY015_WIKI_D28 end as DAY015_WIKI_D28
        , case when DAY014_WIKI_D28 is null then -1 else DAY014_WIKI_D28 end as DAY014_WIKI_D28
        , case when DAY013_WIKI_D28 is null then -1 else DAY013_WIKI_D28 end as DAY013_WIKI_D28
        , case when DAY012_WIKI_D28 is null then -1 else DAY012_WIKI_D28 end as DAY012_WIKI_D28
        , case when DAY011_WIKI_D28 is null then -1 else DAY011_WIKI_D28 end as DAY011_WIKI_D28
        , case when DAY010_WIKI_D28 is null then -1 else DAY010_WIKI_D28 end as DAY010_WIKI_D28
        , case when DAY009_WIKI_D28 is null then -1 else DAY009_WIKI_D28 end as DAY009_WIKI_D28
        , case when DAY008_WIKI_D28 is null then -1 else DAY008_WIKI_D28 end as DAY008_WIKI_D28
        , case when DAY007_WIKI_D28 is null then -1 else DAY007_WIKI_D28 end as DAY007_WIKI_D28
        , case when DAY006_WIKI_D28 is null then -1 else DAY006_WIKI_D28 end as DAY006_WIKI_D28
        , case when DAY005_WIKI_D28 is null then -1 else DAY005_WIKI_D28 end as DAY005_WIKI_D28
        , case when DAY004_WIKI_D28 is null then -1 else DAY004_WIKI_D28 end as DAY004_WIKI_D28
        , case when DAY003_WIKI_D28 is null then -1 else DAY003_WIKI_D28 end as DAY003_WIKI_D28
        , case when DAY002_WIKI_D28 is null then -1 else DAY002_WIKI_D28 end as DAY002_WIKI_D28
        , case when DAY001_WIKI_D28 is null then -1 else DAY001_WIKI_D28 end as DAY001_WIKI_D28
        , case when DAY000_WIKI_D28 is null then -1 else DAY000_WIKI_D28 end as DAY000_WIKI_D28
    from viewed_pivot_table as v
    right join {database}.{schema}.title_retail_funnel_metrics as f
        on v.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                                            else 1 end, '-', f.match_id)
    )
    select
        f.*
    from final as f

) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



