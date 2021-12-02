-- viewership table: source of viewership, either heartbeat or now_uer_stream
-- end_date: the end date of the viewership data

/*create or replace table {database}.{schema}.trailer_retail_view_percent (
      title_name varchar (255) not null
    , match_id varchar (255) not null
    , platform_name varchar (255) not null
    , cumulative_day_num integer
    , total_trailer_num integer
    , match_id_platform varchar (255) not null
    , first_trailer_offered_timestamp timestamp
    , title_offered_timestamp timestamp
    , viewe_count integer
    , total_retail_sub_count integer
    , last_update_timestamp timestamp
    , retail_trailer_view_metric float
    , constraint id_plt_session primary key (match_id, platform_name, match_id_platform)
    , constraint id_plt_session_unique unique (match_id, platform_name, match_id_platform)
)*/

insert into {database}.{schema}.trailer_retail_view_percent (
      title_name 
    , match_id 
    , platform_name 
    , cumulative_day_num 
    , total_trailer_num 
    , match_id_platform 
    , first_trailer_offered_timestamp 
    , title_offered_timestamp 
    , viewe_count 
    , total_retail_sub_count 
    , last_update_timestamp 
    , retail_trailer_view_metric 
    )
  select
          'Timestamp Row'
        , null
        , null
        , null
        , null
        , null
        , null
        , null
        , null
        , null
        , null
        , null
        , null
        , null
        , to_date({end_date})
        , null
);



