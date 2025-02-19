

  create or replace view `just-eat-451011`.`just_eat_dataset`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `just-eat-451011`.`just_eat_dataset`.`my_first_dbt_model`
where id = 1;

