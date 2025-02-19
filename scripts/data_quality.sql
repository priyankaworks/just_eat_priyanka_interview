SELECT 
    COUNT(DISTINCT reviewerID) 
    FROM `just-eat-451011.just_eat_dataset.external_reviews_final`
EXCEPT DISTINCT
SELECT 
    COUNT(DISTINCT reviewerID) 
    FROM `just-eat-451011.just_eat_dataset.stg_reviews`;

SELECT 
    COUNT(DISTINCT metadataid) 
    FROM `just-eat-451011.just_eat_dataset.external_metadata_final`
EXCEPT DISTINCT
SELECT 
    COUNT(DISTINCT metadataid) 
    FROM `just-eat-451011.just_eat_dataset.stg_metadata`;

--count check for reviews 
SELECT 
(SELECT COUNT(*) FROM `just-eat-451011.just_eat_dataset.external_reviews_final`) 
- 
(SELECT COUNT(*) FROM `just-eat-451011.just_eat_dataset.stg_reviews`) 
AS record_count_difference;

-- count check for metadata
SELECT 
    (SELECT COUNT(*) FROM `just-eat-451011.just_eat_dataset.external_metadata_final`) 
    - 
    (SELECT COUNT(*) FROM `just-eat-451011.just_eat_dataset.stg_metadata`) 
    AS record_count_difference;

    -- null chek  for reviews 
SELECT COUNT(*) AS null_count_external
FROM `just-eat-451011.just_eat_dataset.external_reviews_final`
WHERE reviewerID IS NULL OR asin IS NULL ;
SELECT 
    COUNT(*) AS null_count_cleaned
FROM `just-eat-451011.just_eat_dataset.stg_reviews`
WHERE reviewerID IS NULL OR asin IS NULL ;

-- Referential Integrity

SELECT
   fr.asin
FROM
   `just_eat_dataset.Fact_Reviews` fr
LEFT JOIN
   `just_eat_dataset.Dim_Product` dp ON fr.asin = dp.asin
WHERE
   dp.asin IS NULL;

-- Data Range Check

SELECT
   reviewerID,
   asin,
   overall
FROM
   `just_eat_dataset.stg_reviews`
WHERE
   overall < 1 OR overall > 5;

