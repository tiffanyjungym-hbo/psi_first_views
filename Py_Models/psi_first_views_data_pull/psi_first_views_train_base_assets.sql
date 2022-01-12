-- database: db of table
-- schema: schema of table

create or replace table {database}.{schema}.psi_past_base_assets as 
    (select distinct
          rad.title_id
        , coalesce(rad.season_number,0) as season_number
        , rad.viewable_id
        , title_name
        , first_offered_date::date as asset_max_premiere
        , end_utc_max::date as asset_max_end_dt
        , coalesce(raod.season_first_offered_date::date,raod.title_first_offered_date::date) as season_premiere
        , asset_run_time
        , rad.content_category
        , episode_number_in_season
        , content_source
        , program_type
        , category
        , tier
        , viewership_start_date as effective_start_date
        , viewership_end_date as effective_end_date
    from max_prod.catalog.reporting_asset_dim rad
    join max_prod.catalog.reporting_asset_offering_dim raod
        on rad.viewable_id = raod.viewable_id
        and brand = 'HBO MAX'
        and territory = 'HBO MAX DOMESTIC'
        and channel = 'HBO MAX SUBSCRIPTION'
    inner join max_prod.content_analytics.psi_past_title_metadata b
        on rad.title_id = b.viewership_title_id
        and coalesce(rad.season_number,0) = coalesce(b.viewership_season_number,0)
    where 1 = 1
    and asset_type IN ('FEATURE','ELEMENT')
    and start_utc_max is not null
    and rad.content_category in ('movies','series','special')
    and coalesce(raod.season_first_offered_date,raod.title_first_offered_date)  >= '2020-05-27 07:01:00.000'
    order by season_premiere, title_name 
    );