
CREATE OR REPLACE EXTERNAL TABLE `just-eat-451011.just_eat_dataset.external_reviews_final`
(
    reviewerID STRING,
    asin STRING,
    reviewerName STRING,
    helpful STRING,
    reviewText STRING,
    overall FLOAT64,
    summary STRING,
    unixReviewTime STRING,
    reviewTime STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://just_eat_bucket/updated_reviews.csv'],
  skip_leading_rows = 1,
  field_delimiter = ',',
  quote = '"',
  encoding = 'UTF-8'
);

CREATE OR REPLACE TABLE `just-eat-451011.just_eat_dataset.stg_reviews` (
    reviewerID STRING NOT NULL,
    asin STRING NOT NULL,
    reviewerName STRING,
    helpful ARRAY<INT64>,
    reviewText STRING,
    overall INT64,
    summary STRING,
    unixReviewTime INT64,
    reviewDate DATE
) AS
WITH transformed_data AS (
    SELECT 
        CAST(reviewerID AS STRING) AS reviewerID,
        CAST(asin AS STRING) AS asin,
        CAST(reviewerName AS STRING) AS reviewerName,

        -- Convert 'helpful' from a JSON string to ARRAY<INT64>
        ARRAY(
            SELECT CAST(value AS INT64)
            FROM UNNEST(JSON_EXTRACT_ARRAY(helpful)) AS value
        ) AS helpful,

        CAST(reviewText AS STRING) AS reviewText,
        CAST(ROUND(overall) AS INT64) AS overall,
        CAST(summary AS STRING) AS summary,
        CAST(unixReviewTime AS INT64) AS unixReviewTime,

        -- Convert 'reviewTime' from "MM DD, YYYY" to DATE and rename it to reviewDate
        PARSE_DATE('%m %d, %Y', reviewTime) AS reviewDate

    FROM `just-eat-451011.just_eat_dataset.external_reviews_final`
)
SELECT * FROM transformed_data;


------------------------------------Metadata--------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE `just-eat-451011.just_eat_dataset.external_metadata_final`
(
    metadataid STRING,
    asin STRING,
    salesrank STRING,  -- JSON stored as STRING
    imurl STRING,
    categories STRING,
    title STRING,
    description STRING,
    price STRING,
    related STRING,  -- JSON stored as STRING
    brand STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://just_eat_bucket/updated_metadata_cleaned_final.csv'],
  skip_leading_rows = 1, 
  field_delimiter = ',',
  quote = '"',
  encoding = 'UTF-8',
  allow_quoted_newlines = TRUE  -- Allows multi-line fields
);



CREATE OR REPLACE TABLE `just-eat-451011.just_eat_dataset.stg_metadata` (
    metadataid STRING NOT NULL,
    asin STRING NOT NULL,
    salesrank STRING,
    imurl STRING,
    categories STRING,
    title STRING,
    description STRING,
    price FLOAT64,
    related STRING,
    brand STRING
) AS
WITH transformed_metadata AS (
    SELECT 
        CAST(metadataid AS STRING) AS metadataid,
        CAST(asin AS STRING) AS asin,

        CAST(salesrank AS STRING) AS salesrank,
        CAST(imurl AS STRING) AS imurl,

        -- Normalize categories, ensuring empty values are mapped correctly
        COALESCE(NULLIF(categories, ''), 'Not Mapped') AS categories,
        COALESCE(NULLIF(title, ''), 'Not Mapped') AS title,
        COALESCE(NULLIF(description, ''), 'Not Mapped') AS description,

        -- Convert price to FLOAT64, ensuring NULL or empty values are handled
        SAFE_CAST(NULLIF(price, '') AS FLOAT64) AS price,

        CAST(related AS STRING) AS related,
        COALESCE(NULLIF(brand, ''), 'Not Mapped') AS brand
    FROM `just-eat-451011.just_eat_dataset.external_metadata_final`
)
SELECT * FROM transformed_metadata;
