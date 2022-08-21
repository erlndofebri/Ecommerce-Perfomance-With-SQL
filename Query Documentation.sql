-----------------------------------------------
------------------TASK 1-----------------------
-----------------------------------------------

BEGIN;


CREATE TABLE IF NOT EXISTS public.order_items_dataset
(
    order_id character varying COLLATE pg_catalog."default",
    order_item_id character varying COLLATE pg_catalog."default",
    product_id character varying COLLATE pg_catalog."default",
    seller_id character varying COLLATE pg_catalog."default",
    shipping_limit_date date,
    price real,
    freight_value real
);

CREATE TABLE IF NOT EXISTS public.customers_dataset
(
    customer_id character varying COLLATE pg_catalog."default" NOT NULL,
    customer_unique_id character varying COLLATE pg_catalog."default",
    customer_zip_code_prefix character varying COLLATE pg_catalog."default",
    customer_city character varying COLLATE pg_catalog."default",
    customer_state character varying COLLATE pg_catalog."default",
    CONSTRAINT customers_dataset_pkey PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS public.orders_dataset
(
    order_id character varying COLLATE pg_catalog."default" NOT NULL,
    customer_id character varying COLLATE pg_catalog."default",
    order_status character varying COLLATE pg_catalog."default",
    order_purchase_timestamp timestamp without time zone,
    order_approved_at date,
    order_delivered_carrier_date date,
    order_delivered_customer_date date,
    order_estimated_delivery_date date,
    CONSTRAINT orders_dataset_pkey PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS public.order_reviews_dataset
(
    review_id character varying COLLATE pg_catalog."default",
    order_id character varying COLLATE pg_catalog."default",
    review_score integer,
    review_comment_title character varying COLLATE pg_catalog."default",
    review_comment_message character varying COLLATE pg_catalog."default",
    review_creation_date date,
    review_answer_timestamp timestamp without time zone
);

CREATE TABLE IF NOT EXISTS public.order_payments_dataset
(
    order_id character varying COLLATE pg_catalog."default",
    payment_sequential integer,
    payment_type character varying COLLATE pg_catalog."default",
    payment_installments integer,
    payment_value real
);

CREATE TABLE IF NOT EXISTS public.product_dataset
(
    num numeric,
    product_id character varying COLLATE pg_catalog."default" NOT NULL,
    product_category_name character varying COLLATE pg_catalog."default",
    product_name_lenght numeric,
    product_description_lenght numeric,
    product_photos_qty numeric,
    product_weight_g numeric,
    product_length_cm numeric,
    product_height_cm numeric,
    product_width_cm numeric,
    CONSTRAINT product_dataset_pkey PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS public.sellers_dataset
(
    seller_id character varying COLLATE pg_catalog."default" NOT NULL,
    seller_zip_code_prefix character varying COLLATE pg_catalog."default",
    seller_city character varying COLLATE pg_catalog."default",
    seller_state character varying COLLATE pg_catalog."default",
    CONSTRAINT sellers_dataset_pkey PRIMARY KEY (seller_id)
);

CREATE TABLE IF NOT EXISTS public.geolocation_dataset
(
    geolocation_zip_code_prefix character varying COLLATE pg_catalog."default",
    geolocation_lat double precision,
    geolocation_lng double precision,
    geolocation_city character varying COLLATE pg_catalog."default",
    geolocation_state character varying COLLATE pg_catalog."default"
);

ALTER TABLE IF EXISTS public.order_items_dataset
    ADD FOREIGN KEY (order_id)
    REFERENCES public.orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_items_dataset
    ADD FOREIGN KEY (product_id)
    REFERENCES public.product_dataset (product_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_items_dataset
    ADD FOREIGN KEY (seller_id)
    REFERENCES public.sellers_dataset (seller_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.customers_dataset
    ADD FOREIGN KEY (customer_zip_code_prefix)
    REFERENCES public.geolocation_dataset (geolocation_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.orders_dataset
    ADD FOREIGN KEY (customer_id)
    REFERENCES public.customers_dataset (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_reviews_dataset
    ADD FOREIGN KEY (order_id)
    REFERENCES public.orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.order_payments_dataset
    ADD FOREIGN KEY (order_id)
    REFERENCES public.orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.sellers_dataset
    ADD FOREIGN KEY (seller_zip_code_prefix)
    REFERENCES public.geolocation_dataset (geolocation_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;

-----------------------------------------------
------------------TASK 2-----------------------
-----------------------------------------------

-- Annual Average Monthly Active User (MAU)
CREATE TABLE annual_mau_dataset AS
WITH num_cust_with_date AS (
	SELECT DATE_PART('year', od.order_purchase_timestamp) AS year_customer,
	       DATE_PART('month', od.order_purchase_timestamp) AS month_customer,
	       COUNT(DISTINCT cd.customer_unique_id) AS total_customer
	FROM orders_dataset AS od
	JOIN customers_dataset AS cd
	  ON od.customer_id = cd.customer_id
	GROUP BY 1,2
)
SELECT year_customer,
  	   ROUND(AVG(total_customer),0) AS avg_mau_per_year
FROM num_cust_with_date
GROUP BY 1;

-- Annual New Customers
CREATE TABLE annual_new_customer_dataset AS
WITH customer_list AS (
	SELECT cd.customer_unique_id,
		   MIN(od.order_purchase_timestamp) AS first_order
	FROM orders_dataset AS od
	JOIN customers_dataset AS cd 
	  ON cd.customer_id = od.customer_id
	GROUP BY 1
)
SELECT DATE_PART('year', first_order) AS year_customer,
	   COUNT(customer_unique_id) AS new_customers
FROM customer_list
GROUP BY 1
ORDER BY 1;

-- Annual Customer Repeat Order
CREATE TABLE annual_repeat_order_dataset AS
WITH customer_total_order AS (
	SELECT DATE_PART('year', od.order_purchase_timestamp) AS year_customer,
	       cd.customer_unique_id,
		   COUNT(cd.customer_unique_id) AS total_customer,
	       COUNT(od.order_id) AS total_order
	FROM orders_dataset AS od
	JOIN customers_dataset AS cd
	  ON od.customer_id = cd.customer_id
	GROUP BY 1,2
	HAVING COUNT(order_id) > 1
)
SELECT year_customer,
       COUNT(total_customer) AS cust_repeat_order
FROM customer_total_order
GROUP BY 1
ORDER BY 1;

-- Annual Average Order Frequency
CREATE TABLE annual_frequency_order_dataset AS
WITH customer_order AS (
	SELECT DATE_PART('year', od.order_purchase_timestamp) AS year_customer,
	       cd.customer_unique_id,
	       COUNT(DISTINCT order_id) AS total_order
	FROM orders_dataset AS od
	JOIN customers_dataset AS cd
	  ON od.customer_id = cd.customer_id
	GROUP BY 1,2
)
SELECT year_customer,
       ROUND(AVG(total_order),3) AS avg_freq_order
FROM customer_order
GROUP BY 1
ORDER BY 1;



---------JOIN ALL INTO A SINGLE TABEL------------

SELECT mau.year_customer,
	   mau.avg_mau_per_year,
	   newc.new_customers,
	   rep.cust_repeat_order,
	   freq.avg_freq_order
FROM annual_mau_dataset AS mau,
     annual_new_customer_dataset AS newc,
	 annual_repeat_order_dataset AS rep,
	 annual_frequency_order_dataset AS freq
WHERE mau.year_customer = newc.year_customer AND
      newc.year_customer = rep.year_customer AND
	  rep.year_customer = freq.year_customer
	  
-----------------------------------------------
------------------TASK 3-----------------------
-----------------------------------------------

-- Annual Revenue
CREATE TABLE annual_revenue_dataset AS
SELECT DATE_PART('year', od.order_purchase_timestamp) AS year_customer,
	   ROUND(SUM(oid.price + oid.freight_value)) AS revenue
FROM orders_dataset AS od
JOIN order_items_dataset oid 
  ON od.order_id = oid.order_id
WHERE od.order_status = 'delivered'
GROUP BY 1
ORDER BY 1;


-- Annual Num of Order Canceled
CREATE TABLE annual_order_canceled_dataset AS
SELECT DATE_PART('year', order_purchase_timestamp) as year_customer,
  	   COUNT(*) AS num_canceled
FROM orders_dataset
WHERE order_status = 'canceled'
GROUP by 1
ORDER by 1;

--Top Category Revenue
CREATE TABLE annual_top_category_revenue_dataset AS 
WITH category_rank AS (
	SELECT DATE_PART('year', od.order_purchase_timestamp) AS year_customer,
	       pd.product_category_name,
		   ROUND(SUM(oid.price + oid.freight_value)) AS revenue,
	       RANK() OVER(PARTITION BY
					  DATE_PART('year', od.order_purchase_timestamp)
					  ORDER BY ROUND(SUM(oid.price + oid.freight_value)) DESC) AS revenue_rank
	FROM order_items_dataset AS oid
	JOIN orders_dataset AS od
	  ON oid.order_id = od.order_id
	JOIN product_dataset AS pd
	  ON oid.product_id = pd.product_id
	WHERE od.order_status = 'delivered'
	GROUP BY 1,2	
)
SELECT year_customer, 
	   product_category_name, 
	   revenue
FROM category_rank
WHERE revenue_rank = 1;


-- Top Category Canceled
CREATE TABLE annual_top_category_canceled_dataset AS
WITH canceled_category AS (
	SELECT DATE_PART('year', od.order_purchase_timestamp) AS year_customer,
	       pd.product_category_name,
	       COUNT(*) AS num_canceled,
	       RANK() OVER(PARTITION BY
					DATE_PART('year', od.order_purchase_timestamp)
					ORDER BY COUNT(*) DESC)  AS canceled_rank 
	FROM order_items_dataset AS oid
	JOIN orders_dataset AS od 
	  ON oid.order_id = od.order_id
	JOIN product_dataset AS pd
	  ON oid.product_id = pd.product_id
	WHERE od.order_status = 'canceled'
	GROUP BY 1,2
)
SELECT year_customer,
	   product_category_name,
	   num_canceled
FROM canceled_category
WHERE canceled_rank = 1


---------JOIN ALL INTO A SINGLE TABEL------------


SELECT rev.year_customer,
	   rev.revenue,
       can.num_canceled, 
       trev.product_category_name,
       trev.revenue,
       tcan.product_category_name,
       tcan.num_canceled
FROM annual_revenue_dataset AS rev,
 	 annual_order_canceled_dataset AS can,
	 annual_top_category_revenue_dataset AS trev,
	 annual_top_category_canceled_dataset AS tcan
WHERE can.year_customer = rev.year_customer AND
      rev.year_customer = tcan.year_customer AND
      tcan.year_customer = trev.year_customer
	  
	  
	  
-----------------------------------------------
------------------TASK 4-----------------------
-----------------------------------------------


---------- All Time Payment Type -------------

SELECT payment_type,
	   COUNT(1) AS freq_used
FROM order_payments_dataset
GROUP BY payment_type
ORDER BY freq_used DESC;

---------- Annualy Frequency Payment Type -------------

WITH payment_type_freq AS (
	SELECT DATE_PART('year', od.order_purchase_timestamp) AS year_customer,
	       opd.payment_type,
	       COUNT(1) AS frequency_used
	FROM order_payments_dataset AS opd
	JOIN orders_dataset AS od
	  ON opd.order_id = od.order_id
	GROUP BY 1,2
),
payment_freq AS (
select payment_type,
	 		 SUM(CASE WHEN year_customer = 2016 THEN frequency_used ELSE 0 END) AS yr_2016,
	         SUM(CASE WHEN year_customer = 2017 THEN frequency_used ELSE 0 END) AS yr_2017,
	         SUM(CASE WHEN year_customer = 2018 THEN frequency_used ELSE 0 END) AS yr_2018
from payment_type_freq
group by payment_type
)
select *
FROM payment_freq

