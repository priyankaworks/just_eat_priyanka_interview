

  create or replace view `just-eat-451011`.`just_eat_dataset`.`metrics_categories`
  OPTIONS()
  as 

WITH
    ReviewStats
    AS
    (
        SELECT
            d.year_month,
            p.category, -- Get category from Dim_Product
            ROUND(AVG(f.average_rating), 2) AS avg_review_rating,
            SUM(f.review_count) AS total_reviews
        FROM just_eat_dataset.Fact_Reviews f
            LEFT JOIN just_eat_dataset.Dim_Product p
            ON f.asin = p.asin AND p.is_current = TRUE
            LEFT JOIN just_eat_dataset.Dim_Date d
            ON f.date_id = d.date_id
        GROUP BY d.year_month, p.category
    )
SELECT *
FROM ReviewStats
ORDER BY year_month DESC, avg_review_rating DESC;

