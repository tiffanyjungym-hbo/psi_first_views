select distinct
    case
        when f.title_name = 'BARRY LYNDON' then 'Barry Lyndon'
        when f.title_name = 'In The Heights' then 'In the Heights'
        else f.title_name end as title_name
    , imdb_title_id as imdb_id
from max_prod.catalog.asset_dim as asset
join {database}.{schema}.title_retail_funnel_metrics as f
    on f.match_id = asset.viewable_id
join enterprise_data.catalog.wm_catalog as c
    on f.match_id = c.viewable_id
where 1=1
    and asset.content_category in ('movies', 'special')
    and length(imdb_title_id) in (9,10)
    and earliest_offered_timestamp >= dateadd(day, 63, '2018-01-01')
union
select distinct
    f.title_name -- imdb put the seasons of a same series under same page
    , imdb_series_id as imdb_id
from max_prod.catalog.asset_dim as asset
join {database}.{schema}.title_retail_funnel_metrics as f
    on substr(f.match_id, 1, 21) = asset.series_id
join enterprise_data.catalog.wm_catalog as c
    on asset.viewable_id = c.viewable_id
where 1=1
    and asset.content_category in ('series')
    and length(imdb_series_id) in (9,10)
    and earliest_offered_timestamp >= dateadd(day, 63, '2018-01-01')
order by title_name