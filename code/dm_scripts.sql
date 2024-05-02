-- ecomm.dm_book_sales source

CREATE OR REPLACE VIEW ecomm.dm_book_sales
AS SELECT reviews.id AS review_id,
    reviews.title AS review_title,
    reviews.price AS review_price,
    reviews.user_id AS review_user_id,
    reviews.profile_name AS review_profile_name,
    reviews.review_helpfulness,
    reviews.review_score,
    reviews.review_time,
    reviews.review_summary,
    reviews.review_text,
    reviews.label AS review_label,
    reviews.review_summary_words,
    reviews.review_summary_insightful_words,
    reviews.prediction AS review_prediction,
    books.title AS book_title,
    books.description AS book_description,
    books.authors AS book_authors,
    books.image AS book_image,
    books.preview_link AS book_preview_link,
    books.publisher AS book_publisher,
    books.published_date AS book_published_date,
    books.info_link AS book_info_link,
    books.categories AS book_categories,
    books.ratings_count AS book_ratings_count
   FROM ecomm.book_reviews reviews
     LEFT JOIN ecomm.books books ON lower(reviews.title) = lower(books.title);


-- ecomm.dm_electronics_sales source

CREATE OR REPLACE VIEW ecomm.dm_electronics_sales
AS SELECT sales.order_id AS sales_order_id,
    sales.product AS sales_product,
    sales.quantity_ordered AS sales_quantity_ordered,
    sales.price_each AS sales_price_each,
    sales.order_date AS sales_order_date,
    sales.purchase_address AS sales_purchase_address,
    sales.street AS sales_street,
    sales.city AS sales_city,
    sales.state_zip AS sales_state_zip,
    sales.city_state AS sales_city_state,
    sales.state AS sales_state,
    sales.state_full_name AS sales_state_full_name,
    sales.order_year AS sales_order_year,
    sales.order_month AS sales_order_month,
    sales.order_day AS sales_order_day,
    sales.order_hour AS sales_order_hour,
    sales.order_minute AS sales_order_minute,
    sales.order_second AS sales_order_second,
    electronic.product_category AS electronic_product_category,
    electronic.product_type AS electronic_product_type,
    cities.state_abbreviation AS cities_state_abbreviation,
    cities.state_full_name AS cities_state_full_name,
    cities.city AS cities_city,
    cities.county AS cities_county,
    cities.latitude AS cities_latitude,
    cities.longitude AS cities_longitude,
    cities.region AS cities_region
   FROM ecomm.sales sales
     LEFT JOIN ecomm.electronics_category electronic ON sales.product ~~* (('%'::text || electronic.product_type::text) || '%'::text)
     LEFT JOIN ecomm.us_cities_geography cities ON sales.state = cities.state_abbreviation::text;


-- ecomm.dm_liquor_sales source

CREATE OR REPLACE VIEW ecomm.dm_liquor_sales
AS SELECT liquor_sales.invoice_item_number AS liquor_sales_invoice_item_number,
    liquor_sales.date AS liquor_sales_date,
    liquor_sales.store_number AS liquor_sales_store_number,
    liquor_sales.store_name AS liquor_sales_store_name,
    liquor_sales.address AS liquor_sales_address,
    liquor_sales.city AS liquor_sales_city,
    liquor_sales.zip_code AS liquor_sales_zip_code,
    liquor_sales.store_location AS liquor_sales_store_location,
    liquor_sales.county_number AS liquor_sales_county_number,
    liquor_sales.county AS liquor_sales_county,
    liquor_sales.category AS liquor_sales_category,
    liquor_sales.category_name AS liquor_sales_category_name,
    liquor_sales.vendor_number AS liquor_sales_vendor_number,
    liquor_sales.vendor_name AS liquor_sales_vendor_name,
    liquor_sales.item_number AS liquor_sales_item_number,
    liquor_sales.item_description AS liquor_sales_item_description,
    liquor_sales.pack AS liquor_sales_pack,
    liquor_sales.bottle_volume_ml AS liquor_sales_bottle_volume_ml,
    liquor_sales.state_bottle_cost AS liquor_sales_state_bottle_cost,
    liquor_sales.state_bottle_retail AS liquor_sales_state_bottle_retail,
    liquor_sales.state_bottle_retail - liquor_sales.state_bottle_cost AS liquor_sales_state_bottle_profit,
    liquor_sales.bottles_sold AS liquor_sales_bottles_sold,
    liquor_sales.sale_dollars AS liquor_sales_sale_dollars,
    liquor_sales.volume_sold_liters AS liquor_sales_volume_sold_liters,
    liquor_sales.volume_sold_gallons AS liquor_sales_volume_sold_gallons,
    liquor_sales.order_year AS liquor_sales_order_year,
    liquor_sales.order_month AS liquor_sales_order_month,
    liquor_sales.order_day AS liquor_sales_order_day,
    liquor_sales.order_hour AS liquor_sales_order_hour,
    liquor_sales.order_minute AS liquor_sales_order_minute,
    liquor_sales.order_second AS liquor_sales_order_second,
    COALESCE(liquor.liqour_type_1, liquor.liqour_type_2, 'Others'::character varying) AS liqour_type,
    cities.state_abbreviation AS cities_state_abbreviation,
    cities.state_full_name AS cities_state_full_name,
    cities.city AS cities_city,
    cities.county AS cities_county,
    cities.latitude AS cities_latitude,
    cities.longitude AS cities_longitude,
    cities.region AS cities_region
   FROM ecomm.liquor_sales liquor_sales
     LEFT JOIN ecomm.liqour_types liquor ON liquor_sales.category_name ~~* (('%'::text || liquor.liqour_type_1::text) || '%'::text) OR liquor_sales.category_name ~~* (('%'::text || liquor.liqour_type_2::text) || '%'::text)
     LEFT JOIN ecomm.us_cities_geography cities ON liquor_sales.city ~~* cities.city::text;


-- ecomm.dm_sales_all source

CREATE OR REPLACE VIEW ecomm.dm_sales_all
AS SELECT dm_electronics_sales.sales_order_id AS order_id,
    dm_electronics_sales.sales_product AS product_name,
    dm_electronics_sales.sales_quantity_ordered AS quantity_ordered,
    dm_electronics_sales.sales_price_each AS price,
    dm_electronics_sales.sales_order_date AS order_date,
    dm_electronics_sales.sales_purchase_address AS purchase_address,
    dm_electronics_sales.sales_order_year AS order_year,
    dm_electronics_sales.sales_order_month AS order_month,
    dm_electronics_sales.sales_order_day AS order_day,
    dm_electronics_sales.sales_order_hour AS order_hour,
    dm_electronics_sales.sales_order_minute AS order_minute,
    dm_electronics_sales.sales_order_second AS order_second,
    dm_electronics_sales.electronic_product_category AS product_category,
    dm_electronics_sales.cities_state_abbreviation,
    dm_electronics_sales.cities_state_full_name,
    dm_electronics_sales.cities_city,
    dm_electronics_sales.cities_county,
    dm_electronics_sales.cities_latitude,
    dm_electronics_sales.cities_longitude,
    dm_electronics_sales.cities_region
   FROM ecomm.dm_electronics_sales
UNION
 SELECT dm_liquor_sales.liquor_sales_invoice_item_number AS order_id,
    dm_liquor_sales.liquor_sales_category_name AS product_name,
    dm_liquor_sales.liquor_sales_bottles_sold AS quantity_ordered,
    dm_liquor_sales.liquor_sales_state_bottle_cost AS price,
    dm_liquor_sales.liquor_sales_date AS order_date,
    dm_liquor_sales.liquor_sales_address AS purchase_address,
    dm_liquor_sales.liquor_sales_order_year AS order_year,
    dm_liquor_sales.liquor_sales_order_month AS order_month,
    dm_liquor_sales.liquor_sales_order_day AS order_day,
    dm_liquor_sales.liquor_sales_order_hour AS order_hour,
    dm_liquor_sales.liquor_sales_order_minute AS order_minute,
    dm_liquor_sales.liquor_sales_order_second AS order_second,
    dm_liquor_sales.liqour_type AS product_category,
    dm_liquor_sales.cities_state_abbreviation,
    dm_liquor_sales.cities_state_full_name,
    dm_liquor_sales.cities_city,
    dm_liquor_sales.cities_county,
    dm_liquor_sales.cities_latitude,
    dm_liquor_sales.cities_longitude,
    dm_liquor_sales.cities_region
   FROM ecomm.dm_liquor_sales;