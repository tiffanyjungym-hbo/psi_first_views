-- run_date: YYYY-MM-DD string for run date
-- database: db of table
-- schema: schema of table

select distinct
      pba.title_id
    , coalesce(pba.season_number,0) as season_number
    , pba.viewable_id
    , pba.title_name
    , pba.content_category
    , pba.program_type
    , pba.category
    , pba.tier
    , pba.effective_start_date
    , pba.effective_end_date
    , coalesce(ivm.imdb_id, ivm.imdb_series_id) as imdb_imdb_series_id
    , imc.reference_type
    , itr.original_title as reference_title
    , itr.title_id as reference_title_id
    , itr.title_type as reference_title_type
    , imcr.reference_type as reference_reference_type
    , itrr.title_id as reference_reference_title_id
from {database}.{schema}.psi_past_base_assets pba
left join max_prod.editorial.imdb_viewable_map ivm
    on pba.title_id = coalesce(ivm.viewable_id, ivm.viewable_series_id) 
left join enterprise_data.catalog.imdb_title it 
    on coalesce(ivm.imdb_id, ivm.imdb_series_id) = it.title_id
left join enterprise_data.catalog.imdb_movie_connection imc 
    on it.title_id = imc.title_id
left join enterprise_data.catalog.imdb_title itr 
    on itr.title_id = imc.reference_title_id
left join enterprise_data.catalog.imdb_movie_connection imcr
    on itr.title_id = imcr.title_id
    and imcr.reference_type in ('featured_in')
left join enterprise_data.catalog.imdb_title itrr 
    on itrr.title_id = imcr.reference_title_id
where 1 = 1
  and imc.reference_type in ('follows','spin_off_from','remake_of','version_of','featured_in')
order by effective_start_date, title_name
;