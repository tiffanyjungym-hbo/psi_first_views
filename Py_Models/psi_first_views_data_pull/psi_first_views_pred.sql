-- database: db of table
-- schema: schema of table

select distinct
    fp.title as title_name
    , ft.imdb_title_id as imdb_imdb_series_id
    , fp.season as season_number
    , fp.tier
    , fp.category
    , ft.content_category
    , fp.premiere_date as effective_start_date
    , fp.schedule_label
    , it.original_title as imdb_title_name
    , it.number_of_votes as n_votes
    , imc.reference_type
    , itr.original_title as reference_title
    , itr.title_id as reference_title_id
    , itr.title_type as reference_title_type
    , itr.number_of_votes as reference_n_votes
    , imcr.reference_type as reference_reference_type
    , itrr.title_id as reference_reference_title_id
from max_prod.content_analytics.daily_future_programming_schedule fp
left join {database}.{schema}.future_title_imdb_map ft
    on fp.title = ft.title_name
left join enterprise_data.catalog.imdb_title it 
    on ft.imdb_title_id = it.title_id
left join enterprise_data.catalog.imdb_movie_connection imc 
    on it.title_id = imc.title_id
    and imc.reference_type in ('follows','spin_off_from','remake_of','version_of','featured_in')
left join enterprise_data.catalog.imdb_title itr 
    on itr.title_id = imc.reference_title_id
left join enterprise_data.catalog.imdb_movie_connection imcr
    on itr.title_id = imcr.title_id
    and imcr.reference_type in ('featured_in')
left join enterprise_data.catalog.imdb_title itrr 
    on itrr.title_id = imcr.reference_title_id
order by effective_start_date, title_name
;