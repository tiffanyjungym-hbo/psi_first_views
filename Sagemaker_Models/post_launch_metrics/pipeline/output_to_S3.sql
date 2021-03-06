use schema {schema};
--
-- Existing title prediction table
--
create or replace table {database}.{schema}.title_retail_percent_view_d28_prediction_existing (
      title_name string not null
    , match_id string not null
    , match_id_platform string not null
    , platform_name int
    , program_type int
    , percent_view_target float
    , pred_day int
    , percent_view_pred float
    , smape float
    , mae float
    , fold int
    , last_update_timestamp timestamp
); 

-- Create Temp Table
create temp table output_d28_temp_existing(
      title_name string not null
    , match_id string not null
    , match_id_platform string not null
    , platform_name int
    , program_type int
    , percent_view_target float
    , pred_day int
    , percent_view_pred float
    , smape float
    , mae float
    , fold int
);

-- Hard Code Temp Table
copy into output_d28_temp_existing
    from(
        select 
              $1
            , $2
            , $3
            , $4
            , $5
            , $6
            , $7
            , $8
            , $9
            , $10
            , $11
        from {stage}/output_percent_view/existing_title_prediction.csv
        )
    file_format = (type = csv null_if=(''))
    on_error = 'CONTINUE';

insert into {database}.{schema}.title_retail_percent_view_d28_prediction_existing (
    select 
        * 
        , current_timestamp(0) as last_update_timestamp
    from output_d28_temp_existing);

--
-- New title prediction table
--
create or replace table {database}.{schema}.title_retail_percent_view_d28_prediction_new (
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
        from {stage}/output_percent_view/new_title_prediction.csv
        )
    file_format = (type = csv null_if=(''))
    on_error = 'CONTINUE';

insert into {database}.{schema}.title_retail_percent_view_d28_prediction_new (
    select 
        * 
        , current_timestamp(0) as last_update_timestamp
    from output_d28_temp);

