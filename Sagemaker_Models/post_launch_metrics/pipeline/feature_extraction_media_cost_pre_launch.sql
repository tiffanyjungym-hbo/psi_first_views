USE SCHEMA workspace;

COPY INTO {stage}/mp_click_prelaunch_feature.csv
FROM (
    with title_id as (
        select distinct
            title_name
            , platform_name
            , earliest_offered_timestamp
            , concat(case when f.platform_name = 'hboNow' then 0
                    else 1 end, '-', f.match_id) as match_id_platform
        from {database}.{schema}.title_retail_funnel_metrics as f
    ),

    cost_value as (
        select
            case when originals_series_calc in ('HBO MAX GROWTH','HBO NOW','-')
                then custom_dimension_11 else originals_series_calc
                    end as originals_series_calc
            , t.match_id_platform
            , ln(sum(media_cost+1)) as ln_total_media_cost_pre_launch
        from title_id as t
        left join max_prod.datorama.datorama_media_perf_hbomax as am
            on lower(case when originals_series_calc in ('HBO MAX GROWTH','HBO NOW','-')
                then custom_dimension_11 else originals_series_calc end) = lower(get(split(t.title_name, ' S'),0))
                and case when ((hbo_product_calc = 'MAX') or (day>='2020-05-27')) then 'hboMax'
                        else 'hboNow' end = platform_name
        where 1=1
            and hbo_product_calc in ('MAX', 'HBO')
            and day between dateadd(day, -56, t.earliest_offered_timestamp)
                and dateadd(min, -1, t.earliest_offered_timestamp)
            and conversion_tag_name is null
        group by 1,2),

    final as (
        select
            t.match_id_platform
            , sum(ifnull(ln_total_media_cost_pre_launch,-1))
                as ln_total_media_cost_pre_launch
        from title_id as t
        left join cost_value as c
            on t.match_id_platform = c.match_id_platform
        group by 1
        order by ln_total_media_cost_pre_launch desc
    )

    select * from final
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;