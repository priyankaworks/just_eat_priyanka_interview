

WITH
    ReviewStats
    AS
    (
        SELECT
            d.year_month,
            c.categories_name,
            ROUND(AVG(f.average_rating), 2) AS avg_review_rating,
            COUNT(f.review_count) AS total_reviews
        FROM just_eat_dataset.Fact_Reviews f
            LEFT JOIN just_eat_dataset.Dim_Categories c ON f.categories_id = c.categories_id
            LEFT JOIN just_eat_dataset.Dim_Date d ON f.date_id = d.date_id
        GROUP BY d.year_month, c.categories_name
    )
SELECT *
FROM ReviewStats
ORDER BY year_month DESC, avg_review_rating DESC