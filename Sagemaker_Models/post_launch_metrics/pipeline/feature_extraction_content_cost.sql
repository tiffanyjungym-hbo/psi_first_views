use schema workspace;

COPY INTO {stage}/metadata_feature.csv
FROM (
    with originals as (
        select distinct
            concat(mapped_id, case when rad.content_category = 'series'
                then concat('-',to_char(ifnull(season, '1'))) else '' end) as match_id
            , to_double(replace(efc, '$', '')) as cost
        from max_dev.content_revenue.hbo_op_cd as h
        left join max_dev.content_revenue.cd_title_map as t
            on h.program = t.base_title
        left join max_prod.catalog.reporting_asset_dim as rad
            on mapped_id = rad.title_id
    ),

    acquired as (
        select distinct
            concat(mapped_id, case when rad.content_category = 'series'
                then concat('-',to_char(ifnull(h.season_number, '1'))) else '' end) as match_id
            , cost/1000 as cost
        from max_dev.content_revenue.hbomax_cd as h
        left join max_dev.content_revenue.cd_title_map as t
            on h.show_name = t.base_title
        left join max_prod.catalog.reporting_asset_dim as rad
            on mapped_id = rad.title_id
    ),

    table_union as (
        select
            *
        from originals
        union
        select
            *
        from acquired
    ),

    final as (select distinct
        concat(case when platform_name = 'hboNow' then 0
                        else 1 end, '-', f.match_id) as match_id_platform
        , max(case when cost is null then -1 else cost end) as content_cost
    from {database}.{schema}.title_retail_funnel_metrics as f
    left join table_union as t
        on f.match_id = t.match_id
    group by 1
    order by 1)

    select
        match_id_platform
        , case
            when match_id_platform = '1-GX9KHPw1OIMPCJgEAAAAD' then 200
            when match_id_platform = '1-GYA79hQZbUsI3gQEAAAB0' then 50
            when match_id_platform = '1-GYBmsKA4FaUnDdQEAAAAj' then 25
            when match_id_platform = '1-GYFEzmwNES16GkQEAAAAC' then 165
            when match_id_platform = '1-GYDAnZgCFQ8IJpQEAAAAN' then 70
            else content_cost
            end as content_cost
    from final
    order by content_cost desc
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;