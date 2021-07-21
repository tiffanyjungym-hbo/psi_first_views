use schema workspace;

COPY INTO {stage}/input_percent_view/cost_feature.csv
FROM (
    with cost_base as (
    select
        concat(title_id, case when c.content_category = 'series'
            then concat('-',to_char(ifnull(right(season_or_movie_title, 1), '1'))) else '' end) as match_id
        , total_cost/1000000 as content_cost
    from  max_dev.content_revenue.content_efficiency_main as c
    )

    select distinct
        concat(case when platform_name = 'hboNow' then 0
                        else 1 end, '-', f.match_id) as match_id_platform
        , ifnull(max(content_cost),-1) as content_cost
    from  {database}.{schema}.title_retail_funnel_metrics as f
    left join cost_base as c
        on f.match_id = c.match_id
    group by 1
    order by content_cost desc
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;