/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

With parent_query
As
(select 
    F.amount,
    D.country
from 
    workspace.gold.FactBookings F
Left Join
    workspace.gold.DimAirports D
on F.DimAirportsKey = D.DimAirportskey)
select country, sum(amount) AS total_amount
from parent_query
group by country


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
