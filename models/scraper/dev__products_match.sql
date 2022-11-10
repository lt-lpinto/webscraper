
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

WITH allinone AS 
  (SELECT DISTINCT * FROM `webscraper.raw__allinone`)
SELECT DISTINCT
 b.title as title,
 b.price as price,
 b.product_url as product_url,
 b.reviews as reviews
FROM `webscraper.raw__allinone_popup_links` b
JOIN allinone a ON lower(a.title) = lower(b.title)

