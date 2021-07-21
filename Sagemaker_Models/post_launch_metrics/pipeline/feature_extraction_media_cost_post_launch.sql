USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/media_cost_postlaunch_feature.csv
FROM (
    with title_id as (
        select distinct
            title_name
            , platform_name
            , earliest_offered_timestamp
            , days_since_first_offered
            , concat(case when f.platform_name = 'hboNow' then 0
                    else 1 end, '-', f.match_id) as match_id_platform
        from {database}.{schema}.title_retail_funnel_metrics as f
    ),

    mk_title_clean as (
        select distinct
            case when originals_series_calc in ('HBO MAX GROWTH','HBO NOW','-')
                    then custom_dimension_11
                when originals_series_calc = 'LOVECRAFT'
                    then 'lovecraft country'
                when originals_series_calc = 'PLOT AGAINST AMERICA'
                    then 'the plot against america'
                when originals_series_calc = 'FRESH PRINCE REUNION'
                    then 'the fresh prince of bel-air reunion'
                when originals_series_calc = 'FLIGHT ATTENDANT'
                    then 'the flight attendant'
                when originals_series_calc = 'FRIENDS (REUNION SPECIAL)'
                    then 'friends: the reunion'
                else originals_series_calc
                        end as originals_series_calc
            , day
            , media_cost
            , hbo_product_calc
        from max_prod.datorama.datorama_media_perf_hbomax as am
        where 1=1
            and hbo_product_calc in ('MAX', 'HBO')
            and conversion_tag_name is null
    ),

    mk_value as (
        select
            originals_series_calc
            , t.match_id_platform
            , datediff(day, t.earliest_offered_timestamp, day) as days_since_first_offered
            , sum(media_cost) as total_media_cost_from_marketing_spend
        from title_id as t
        left join mk_title_clean as am
            on lower(originals_series_calc) = lower(get(split(t.title_name, ' S'),0))
                and case when ((hbo_product_calc = 'MAX') or (day>='2020-05-27')) then 'hboMax'
                    else 'hboNow' end = platform_name
                and datediff(day, to_date(t.earliest_offered_timestamp), day) = t.days_since_first_offered
        where 1=1
            and day >= t.earliest_offered_timestamp
    group by 1,2,3 ),

    total_mk_spend as (
        select
            t.match_id_platform
            , concat('DAY', lpad(to_char(t.days_since_first_offered),3, 0), '_MC') as days_since_first_offered
            , sum(total_media_cost_from_marketing_spend)
                as total_media_cost_from_marketing_spend
        from title_id as t
        left join mk_value as c
            on t.match_id_platform = c.match_id_platform
                and c.days_since_first_offered = t.days_since_first_offered
        where 1=1
            and t.days_since_first_offered<=28
            --and t.match_id_platform in ('1-GYBllVQbyTkOLlAEAAAAC-1','1-GYCiC1Q8picLCfAEAAAAC-1')
        group by 1, 2
        order by match_id_platform, days_since_first_offered,
                total_media_cost_from_marketing_spend desc
    ),

    pivot_base as (
            select
                match_id_platform
                , days_since_first_offered
                , sum(total_media_cost_from_marketing_spend) over
                    (partition by match_id_platform order by days_since_first_offered)
                        as cum_total_media_cost_from_marketing_spend
            from total_mk_spend
            order by match_id_platform, days_since_first_offered
        ),

    null_val_proc as (
        select
            match_id_platform
            , days_since_first_offered
            , case when ((cum_total_media_cost_from_marketing_spend is null)
                            or (cum_total_media_cost_from_marketing_spend<0)) then -1
                else ln(cum_total_media_cost_from_marketing_spend+1) end
                    as ln_cum_total_media_cost
        from pivot_base
    ),

    spend_pivot_table as (
        select
            *
        from null_val_proc
        pivot(mode(ln_cum_total_media_cost) for
            days_since_first_offered in (
                    'DAY001_MC'
                    , 'DAY002_MC'
                    , 'DAY003_MC'
                    , 'DAY004_MC'
                    , 'DAY005_MC'
                    , 'DAY006_MC'
                    , 'DAY007_MC'
                    , 'DAY008_MC'
                    , 'DAY009_MC'
                    , 'DAY010_MC'
                    , 'DAY011_MC'
                    , 'DAY012_MC'
                    , 'DAY013_MC'
                    , 'DAY014_MC'
                    , 'DAY015_MC'
                    , 'DAY016_MC'
                    , 'DAY017_MC'
                    , 'DAY018_MC'
                    , 'DAY019_MC'
                    , 'DAY020_MC'
                    , 'DAY021_MC'
                    , 'DAY022_MC'
                    , 'DAY023_MC'
                    , 'DAY024_MC'
                    , 'DAY025_MC'
                    , 'DAY026_MC'
                    , 'DAY027_MC'
                    , 'DAY028_MC'
                ))
        as mc
        order by match_id_platform
    )

    select * from spend_pivot_table
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;