USE SCHEMA workspace;

COPY INTO {stage}/hourspct/vtp_feature.csv
FROM (

    with base_info_table as (
        select distinct
            match_id
            , days_since_first_offered
            , case when retail_played_count_percent is not null and retail_played_count_percent>0
                    then retail_viewed_count_percent/retail_played_count_percent
                    else 0
                end as viewed_through_portion
        from {database}.{schema}.title_retail_funnel_metrics

    ),

    min_days_since_offered_table as (
        select
            match_id
            , min(days_since_first_offered) as min_days_since_offered
        from base_info_table
        group by 1

    ),

    vpt_pivot_base as (
        select
            b.match_id
            , concat('DAY', lpad(to_char(days_since_first_offered),3, 0), '_VTP') as days_since_first_offered
            , viewed_through_portion
        from base_info_table as b
        join min_days_since_offered_table as md
            on md.match_id = b.match_id
    ),

    vpt_pivot_table as (
        select
            *
        from vpt_pivot_base
        pivot(mode(viewed_through_portion) for
            days_since_first_offered in (
                    'DAY001_VTP'
                    , 'DAY002_VTP'
                    , 'DAY003_VTP'
                    , 'DAY004_VTP'
                    , 'DAY005_VTP'
                    , 'DAY006_VTP'
                    , 'DAY007_VTP'
                    , 'DAY008_VTP'
                    , 'DAY009_VTP'
                    , 'DAY010_VTP'
                    , 'DAY011_VTP'
                    , 'DAY012_VTP'
                    , 'DAY013_VTP'
                    , 'DAY014_VTP'
                    , 'DAY015_VTP'
                    , 'DAY016_VTP'
                    , 'DAY017_VTP'
                    , 'DAY018_VTP'
                    , 'DAY019_VTP'
                    , 'DAY020_VTP'
                    , 'DAY021_VTP'
                    , 'DAY022_VTP'
                    , 'DAY023_VTP'
                    , 'DAY024_VTP'
                    , 'DAY025_VTP'
                    , 'DAY026_VTP'
                    , 'DAY027_VTP'
                    , 'DAY028_VTP'
                ))
        as vpt
        order by match_id
    )

    select
        v.*
    from vpt_pivot_table as v

    order by match_id

) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



