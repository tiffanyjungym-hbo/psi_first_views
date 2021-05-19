use schema {schema};

-- Last Prediction table
create or replace table {database}.{schema}.title_retail_percent_view_d28_prediction (
      title_name string not null
    , match_id string not null
    , match_id_platform string not null
    , platform_name int
    , program_type int
    , pred_day int
    , percent_view_pred float
    , last_update_timestamp timestamp
); 

-- Create Temp Table
create temp table output_d28_temp(
      title_name string not null
    , match_id string not null
    , match_id_platform string not null
    , platform_name int
    , program_type int
    , pred_day int
    , percent_view_pred float
);

-- Hard Code Temp Table
copy into output_d28_temp
    from(
        select 
              $1
            , $2
            , $3
            , $4
            , $5
            , $6
            , $7
        from {stage}/
        )
    file_format = (type = csv null_if=(''))
    on_error = 'CONTINUE';

insert into {database}.{schema}.title_retail_percent_view_d28_prediction (
    select 
        * 
        , current_timestamp(0) as last_update_timestamp
    from output_d28_temp);