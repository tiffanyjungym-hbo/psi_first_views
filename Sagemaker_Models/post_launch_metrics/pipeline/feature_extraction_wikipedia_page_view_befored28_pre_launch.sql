USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/wiki_view_feature_before28.csv
FROM (
    with tmp as (
        select
            match_id_platform
            , sum(total_page_views) as wiki_befored28_total
        from {database}.{schema}.title_wikipedia_page_view as v
        where v.days_since_first_offered < -27
        group by 1
        )

    select distinct
        concat(case when t.platform_name = 'hboNow' then 0
            else 1 end, '-', t.match_id) as match_id_platform
        , ifnull(wiki_befored28_total, -1) as wiki_befored28_total
    from tmp
    right join {database}.{schema}.title_retail_funnel_metrics as t
        on concat(case when t.platform_name = 'hboNow' then 0
            else 1 end, '-', t.match_id) = tmp.match_id_platform
    order by wiki_befored28_total desc

) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



