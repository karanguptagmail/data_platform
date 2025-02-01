-- models/test_macro.sql

{{ macro_get_tag_column() }}

-- Optionally, return some dummy data to ensure the model runs successfully
select current_date() as now
