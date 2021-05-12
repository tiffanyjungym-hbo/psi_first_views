use schema {schema};

-- Last Prediction table
create or replace table {database}.{schema}.title_retail_percent_view_d28_prediction (
    title_name string,
    match_id string,
    match_id_platform string,
    platform_name int,
    program_type int,
    pred_day int,
    percent_view_pred float,
    last_update_timestamp timestamp
); 

-- Create Temp Table
create temp table output_d28_temp(
    title_name string,
    match_id string,
    match_id_platform string,
    platform_name int,
    program_type int,
    pred_day int,
    percent_view_pred float,
);

-- Hard Code Temp Table
copy into output_d28_temp
  from(
            select 
                $1:title_name,
                $1:match_id,
                $1:match_id_platform,
                $1:platform_name,
                $1:program_type,
                $1:pred_day,
                $1:percent_view_pred
            from {stage}/
        )
  file_format = (type = csv null_if=(''))
  on_error = 'CONTINUE';

insert into {database}.{schema}.title_retail_percent_view_d28_prediction
(select 
    * 
    , curret_timestamp(0) as last_update_timestamp
 from output_d28_temp);