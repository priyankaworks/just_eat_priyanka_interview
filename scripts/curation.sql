
--DDL Scripts
CREATE OR REPLACE TABLE just_eat_dataset.Dim_Date (
    date_id STRING,  -- Surrogate Key
    date_value DATE NOT NULL,  
    year INT64 NOT NULL,  
    month INT64 NOT NULL,  
    year_month STRING NOT NULL  
)
PARTITION BY date_value;  -- Partitioning for Performance


-- Using CTE to structure the query for data insertion

MERGE just_eat_dataset.Dim_Date AS target
USING (
    SELECT DISTINCT
        FORMAT_DATE('%Y%m%d', reviewDate) AS date_id,  
        DATE(reviewDate) AS date_value,
        EXTRACT(YEAR FROM reviewDate) AS year,
        EXTRACT(MONTH FROM reviewDate) AS month,
        FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP(reviewDate)) AS year_month
    FROM just-eat-451011.just_eat_dataset.stg_reviews
) AS source
ON target.date_id = source.date_id
WHEN NOT MATCHED THEN
    INSERT (date_id, date_value, year, month, year_month)
    VALUES (source.date_id, source.date_value, source.year, source.month, source.year_month);





-------------------------------------------------Combined Product and Category SCD Type 2 ------------------------------------------
--DDL SCRIPTS
CREATE OR REPLACE TABLE just_eat_dataset.Dim_Product (
    asin STRING NOT NULL,  
    title STRING,
    price FLOAT64,
    image_url STRING,
    brand STRING NOT NULL,
    category STRING NOT NULL,
    start_date TIMESTAMP NOT NULL,  
    end_date TIMESTAMP NOT NULL,  
    is_current BOOLEAN NOT NULL  
);

-- Bulk load initial data from staging
INSERT INTO just_eat_dataset.Dim_Product
(asin, title, price, image_url, brand, category, start_date, end_date, is_current)
SELECT 
  asin,
  title,
  price,
  imurl,
  brand,
  categories,
  CURRENT_TIMESTAMP() AS start_date,
  TIMESTAMP '9999-12-31 23:59:59' AS end_date,
  TRUE AS is_current
FROM `just_eat_dataset.stg_metadata`;

-- 1. Create a view to standardize and deduplicate your staging data
CREATE OR REPLACE VIEW `just_eat_dataset.v_product_raw` AS
SELECT 
  asin,
  COALESCE(NULLIF(title, ''), 'Not Mapped') AS title,
  COALESCE(SAFE_CAST(REGEXP_REPLACE(CAST(price AS STRING), r'[^0-9.]', '') AS FLOAT64), 0.0) AS price,
  imurl AS image_url,
  COALESCE(NULLIF(brand, ''), 'Not Mapped') AS brand,
  COALESCE(NULLIF(categories, ''), 'Not Mapped') AS category,
  CURRENT_TIMESTAMP() AS dw_ingest_timestamp,
  -- Optionally compute a hash for all key fields (helpful for detecting changes)
  MD5(CONCAT(
      COALESCE(title, ''),
      COALESCE(CAST(price AS STRING), ''),
      COALESCE(imurl, ''),
      COALESCE(brand, ''),
      COALESCE(categories, '')
  )) AS m_hash
FROM `just_eat_dataset.stg_metadata`
WHERE asin IS NOT NULL;


-- 2. Insert new products (those that do not already exist in the target)
BEGIN
  INSERT INTO `just_eat_dataset.Dim_Product`
    (asin, title, price, image_url, brand, category, start_date, end_date, is_current)
  SELECT 
    a.asin,
    a.title,
    a.price,
    a.image_url,
    a.brand,
    a.category,
    CURRENT_TIMESTAMP() AS start_date,
    TIMESTAMP '9999-12-31 23:59:59' AS end_date,
    TRUE AS is_current
  FROM `just_eat_dataset.v_product_raw` a
  WHERE NOT EXISTS (
    SELECT 1 FROM `just_eat_dataset.Dim_Product` b
    WHERE b.asin = a.asin
  );
END;

-- 3. Update active records that have changes in any attribute by expiring them
BEGIN
  UPDATE `just_eat_dataset.Dim_Product` AS target
  SET 
    end_date = CURRENT_TIMESTAMP(),
    is_current = FALSE
  WHERE is_current = TRUE
    AND EXISTS (
      SELECT 1
      FROM `just_eat_dataset.v_product_raw` a
      WHERE a.asin = target.asin
        AND (
          target.title      <> a.title OR 
          target.price      <> a.price OR 
          target.image_url  <> a.image_url OR
          target.brand      <> a.brand OR
          target.category   <> a.category
        )
    );
END;


-- 4. Insert new records (new active versions) for products that changed
BEGIN
  INSERT INTO `just_eat_dataset.Dim_Product`
    (asin, title, price, image_url, brand, category, start_date, end_date, is_current)
  SELECT 
    a.asin,
    a.title,
    a.price,
    a.image_url,
    a.brand,
    a.category,
    CURRENT_TIMESTAMP() AS start_date,
    TIMESTAMP '9999-12-31 23:59:59' AS end_date,
    TRUE AS is_current
  FROM `just_eat_dataset.v_product_raw` a
  -- Join with the latest record per product in the target table:
  JOIN (
    SELECT asin, MAX(start_date) AS last_start_date
    FROM `just_eat_dataset.Dim_Product`
    GROUP BY asin
  ) latest
    ON a.asin = latest.asin
  JOIN `just_eat_dataset.Dim_Product` c
    ON latest.asin = c.asin AND latest.last_start_date = c.start_date
  WHERE c.is_current = FALSE
    AND (
         c.title      <> a.title OR 
         c.price      <> a.price OR 
         c.image_url  <> a.image_url OR
         c.brand      <> a.brand OR
         c.category   <> a.category
    );
END;

---------------------------------------------------------FACT REVIEWS TABLE - STAR SCHEMA COMPLIANT --------------------------------------------------------------------------------

--DDL Scripts
CREATE OR REPLACE TABLE just_eat_dataset.Fact_Reviews (
    reviewer_rating_pk STRING NOT NULL,  -- Composite Primary Key
    reviewerID STRING NOT NULL,  
    asin STRING NOT NULL, 
    date_id STRING NOT NULL,  
    date_value DATE NOT NULL,  
    unixReviewTime INT64 NOT NULL,  
    average_rating FLOAT64 NOT NULL,  -- Avg. review rating per product-date-time
    review_count INT64 NOT NULL,  -- Total review count per product-date-time
    start_date DATE NOT NULL,  
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date_value  
CLUSTER BY asin;  


MERGE just_eat_dataset.Fact_Reviews AS target
USING (
    WITH FactCTE AS (
        SELECT 
            
            TO_HEX(MD5(CONCAT(
                COALESCE(r.asin, 'UNKNOWN'), '~',  
                COALESCE(r.reviewerID, 'UNKNOWN'), '~', 
                COALESCE(CAST(r.reviewDate AS STRING), '1970-01-01'), '~',
                COALESCE(CAST(r.unixReviewTime AS STRING), '0') 
            ))) AS reviewer_rating_pk,
            r.reviewerID,
            r.asin,  
            d.date_id,
            d.date_value,  
            r.unixReviewTime,  
            ROUND(AVG(r.overall), 2) AS average_rating,  
            COUNT(r.overall) AS review_count,  
            r.reviewDate,  
            CURRENT_DATE AS start_date  
            
        FROM just-eat-451011.just_eat_dataset.stg_reviews r

        
        LEFT JOIN just_eat_dataset.Dim_Product p  
            ON r.asin = p.asin AND p.is_current = TRUE  

        
        LEFT JOIN just_eat_dataset.Dim_Date d  
            ON DATE(r.reviewDate) = d.date_value  

        GROUP BY 
            r.reviewerID, 
            r.asin,  
            r.reviewDate,  
            r.unixReviewTime,  
            d.date_id, 
            d.date_value
    )
    SELECT * FROM FactCTE
) AS source
ON target.reviewer_rating_pk = source.reviewer_rating_pk  

WHEN MATCHED THEN
    UPDATE SET 
        target.average_rating = source.average_rating,  
        target.review_count = source.review_count,  
        target.start_date = source.start_date

WHEN NOT MATCHED THEN
    INSERT (reviewer_rating_pk, reviewerID, asin, date_id, date_value, unixReviewTime, average_rating, review_count, start_date)
    VALUES (source.reviewer_rating_pk, source.reviewerID, source.asin, source.date_id, source.date_value, source.unixReviewTime, source.average_rating, source.review_count, source.start_date);


