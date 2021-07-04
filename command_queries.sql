/*CREATE TEMPORARY TABLES INTO STAGING AREA*/

CREATE TABLE covid_tmp (covid_info VARIANT);

CREATE TABLE tip_tmp (tip_info VARIANT);

CREATE TABLE user_tmp (user_info VARIANT);

CREATE TABLE review_tmp (review_info VARIANT);

CREATE TABLE business_tmp (busines_info VARIANT);

CREATE TABLE checkin_tmp (checkin_info VARIANT);

CREATE TABLE precipitation_tmp (
	date_tmp STRING,
	precipitation_tmp STRING,
	precipitation_normal STRING);
	
CREATE TABLE temperature_tmp (
	period_tmp STRING,
	min_value_tmp STRING,
	max_value_tmp STRING,
	normal_min_tmp STRING,
	normal_max_tmp STRING);
	

/*COPY FILES INTO TEMPORARY TABLES FROM @proj_stage */

COPY INTO covid_tmp FROM @proj_stage/yelp_academic_dataset_covid_features.json file_format=(type=JSON);

COPY INTO tip_tmp FROM @proj_stage/yelp_academic_dataset_tip.json file_format=(type=JSON);

COPY INTO user_tmp FROM @proj_stage/yelp_academic_dataset_user.json file_format=(type=JSON);

COPY INTO review_tmp FROM @proj_stage/yelp_academic_dataset_review.json file_format=(type=JSON);

COPY INTO business_tmp FROM @proj_stage/yelp_academic_dataset_business.json file_format=(type=JSON);

COPY INTO checkin_tmp FROM @proj_stage/yelp_academic_dataset_checkin.json file_format=(type=JSON);

COPY INTO precipitation_tmp FROM @proj_stage/USW00023169-LAS_VEGAS_MCCARRAN_INTL_AP-precipitation-inch.csv file_format=(type=csv field_delimiter=',' skip_header=1);

COPY INTO temperature_tmp FROM @proj_stage/USW00023169-temperature-degreeF.csv file_format=(type=csv field_delimiter=',' skip_header=1);

UPDATE PRECIPITATION_TMP SET precipitation_tmp = 8888 WHERE precipitation_tmp = 'T';



/*CREATION OF TABLE IN ODS AND TRANSFORM/TRANSFER THE RAW DATA FROM STAGING TO ODS*/

CREATE TABLE precipitation (
	date_t DATE,
	precipitation FLOAT,
	precipitation_normal FLOAT);
	
	
INSERT INTO precipitation(date_t, precipitation, precipitation_normal)
SELECT TO_DATE(date_tmp,'YYYYMMDD'), 
CAST(precipitation_tmp AS FLOAT), 
CAST(precipitation_normal AS FLOAT) FROM "PROJECT_UDACITY"."STAGING".precipitation_tmp;

CREATE TABLE temperature (
	date_t DATE,
	min_t INT,
	max_t INT,
	normal_min FLOAT,
	normal_max FLOAT);
	
INSERT INTO temperature(date_t, min_t, max_t, normal_min, normal_max)
SELECT TO_DATE(period_tmp, 'YYYYMMDD'),
CAST(min_value_tmp AS INT),
CAST(max_value_tmp AS INT),
CAST(normal_min_tmp AS FLOAT),
CAST(normal_max_tmp AS FLOAT) FROM "PROJECT_UDACITY"."STAGING".temperature_tmp;


CREATE TABLE tip (
	business_id STRING,
	compliment_count INTEGER,
	date STRING,
	text STRING,
	user_id STRING);
	
INSERT INTO tip(business_id, compliment_count, date, text, user_id) 
SELECT parse_json($1):business_id,
			parse_json($1):compliment_count,
			parse_json($1):date,
			parse_json($1):text,
			parse_json($1):user_id
	FROM "PROJECT_UDACITY"."STAGING".tip_tmp;
	
	
CREATE TABLE business (
	business_id VARCHAR(200),
	name VARCHAR(100),
	address VARCHAR(200),
	city VARCHAR(50),
	state VARCHAR(10),
	postal_code VARCHAR(20),
	latitude FLOAT,
	longitude FLOAT,
	stars FLOAT,
	review_count NUMBER(38,0),
	is_open NUMBER(38,0),
	attribute OBJECT,
	categories VARCHAR,
	hours VARIANT);
	
INSERT INTO business(business_id,name, address, city, state, postal_code, latitude,longitude,stars, review_count, is_open, attribute, categories, hours)
SELECT parse_json($1):business_id,
			parse_json($1):name,
			parse_json($1):address,
			parse_json($1):city,
			parse_json($1):state,
			parse_json($1):postal_code,
			parse_json($1):latitude,
			parse_json($1):longitude,
			parse_json($1):stars,
			parse_json($1):review_count,
			parse_json($1):is_open,
			parse_json($1):attribute,
			parse_json($1):categories,
			parse_json($1):hours
	FROM "PROJECT_UDACITY"."STAGING".business_tmp;
	
	
CREATE TABLE user (
	average_stars FLOAT,
	compliment_cool NUMBER,
	compliment_cute NUMBER,
	compliment_funny NUMBER,
	compliment_hot NUMBER,
	compliment_list NUMBER,
	compliment_more NUMBER,
	compliment_note NUMBER,
	compliment_photos NUMBER,
	compliment_plain NUMBER,
	compliment_profile NUMBER,
	compliment_writer NUMBER,
	cool NUMBER,
	elite STRING,
	fans NUMBER,
	friends VARIANT,
	funny NUMBER,
	name VARCHAR,
	review_count NUMBER,
	useful NUMBER,
	user_id VARCHAR,
	yelping_since STRING);
	

INSERT INTO user(average_stars,compliment_cool,compliment_cute,compliment_funny,compliment_hot,compliment_list,compliment_more,
compliment_note,compliment_photos,compliment_plain,compliment_profile,compliment_writer,cool,elite, fans,friends, funny,name, review_count, useful,user_id,yelping_since)
SELECT parse_json($1):average_stars,
	parse_json($1):compliment_cool,
	parse_json($1):compliment_cute,
	parse_json($1):compliment_funny,
	parse_json($1):compliment_hot,
	parse_json($1):compliment_list,
	parse_json($1):compliment_more,
	parse_json($1):compliment_note,
	parse_json($1):compliment_photos,
	parse_json($1):compliment_plain,
	parse_json($1):compliment_profile,
	parse_json($1):compliment_writer,
	parse_json($1):cool,
	parse_json($1):elite, 
	parse_json($1):fans,
	parse_json($1):friends, 
	parse_json($1):funny,
	parse_json($1):name, 
	parse_json($1):review_count, 
	parse_json($1):useful,
	parse_json($1):user_id,
	parse_json($1):yelping_since
	FROM "PROJECT_UDACITY"."STAGING".user_tmp;
	
	
CREATE TABLE review(
  business_id VARIANT,
  cool NUMBER,
  date STRING,
  funny NUMBER,
  review_id VARIANT,
  stars NUMBER,
  text STRING,
  useful NUMBER,
  user_id VARIANT);
  
    
INSERT INTO review(business_id, cool, date, funny, review_id, stars, text, useful, user_id) 
SELECT parse_json($1):business_id, 
	parse_json($1):cool, 
	parse_json($1):date, 
	parse_json($1):funny, 
	parse_json($1):review_id, 
	parse_json($1):stars, 
	parse_json($1):text, 
	parse_json($1):useful, 
	parse_json($1):user_id FROM "PROJECT_UDACITY"."STAGING".review_tmp
	
	
CREATE TABLE checkin(
	business_id VARIANT,
	date STRING);
	
INSERT INTO checkin(business_id, date)
SELECT parse_json($1):business_id,
		parse_json($1):date 
		FROM "PROJECT_UDACITY"."STAGING".checkin_tmp;
		
		
CREATE TABLE covid (
	Call_To_Action_enabled VARIANT,
	Covid_Banner VARIANT,
	Grubhub_enabled	VARIANT,
	Request_a_Quote_Enabled VARIANT,
	Temporary_Closed_Until	VARIANT,
	Virtual_Services_Offered VARIANT,
	business_id VARIANT,
	delivery_or_takeout VARIANT,
	highlights VARIANT);

INSERT INTO covid(Call_To_Action_enabled,Covid_Banner,Grubhub_enabled,Request_a_Quote_Enabled, Temporary_Closed_Until,
Virtual_Services_Offered,business_id,delivery_or_takeout,highlights)
SELECT 
	parse_json($1):"Call To Action enabled",
	parse_json($1):"Covid Banner",
	parse_json($1):"Grubhub enabled",
	parse_json($1):"Request a Quote Enabled",
	parse_json($1):"Temporary Closed Until",
	parse_json($1):"Virtual Services Offered",
	parse_json($1):"business_id",
	parse_json($1):"delivery or takeout",
	parse_json($1):"highlights" FROM "PROJECT_UDACITY"."STAGING".covid_tmp;
	
	
--some personal modifications for the tables---

ALTER TABLE precipitation RENAME COLUMN date_t TO date_p;
ALTER TABLE business RENAME COLUMN name TO name_business;
ALTER TABLE checkin RENAME COLUMN date TO date_checkin;
ALTER TABLE review RENAME COLUMN date TO date_review;
ALTER TABLE user RENAME COLUMN name TO name_user;
ALTER TABLE tip RENAME COLUMN date TO date_tip;

ALTER TABLE business RENAME COLUMN business_id TO business_id_main;
ALTER TABLE user RENAME COLUMN user_id TO user_id_main;



	
/*SQL queries code to integrate climate and Yelp Data*/

SELECT * 
	FROM precipitation AS p
	JOIN review AS r 
	ON r.date_review = p.date_p
	JOIN temperature AS t
	ON t.date_t = r.date_review
	JOIN business AS b
	ON b.business_id_main = r.business_id
    JOIN covid AS c
	ON b.business_id_main = c.business_id
	JOIN checkin AS ch
	ON b.business_id_main = ch.business_id
	JOIN tip AS x
	ON b.business_id_main = x.business_id
    JOIN user AS u
    ON u.user_id_main = r.user_id;
	
/* SQL QUERY TO CREATE A FACT TABLE*/	

CREATE TABLE total_info AS SELECT business_id_main, review_id, user_id_main, date_t, date_p
	FROM precipitation AS p
	JOIN review AS r 
	ON r.date_review = p.date_p
	JOIN temperature AS t
	ON t.date_t = r.date_review
	JOIN business AS b
	ON b.business_id_main = r.business_id
    JOIN covid AS c
	ON b.business_id_main = c.business_id
	JOIN checkin AS ch
	ON b.business_id_main = ch.business_id
	JOIN tip AS x
	ON b.business_id_main = x.business_id
    JOIN user AS u
    ON u.user_id_main = r.user_id;
	

/*SQL queries code to move data from ODS to DWH*/

CREATE TABLE covid CLONE "PROJECT_UDACITY"."ODS".covid ;

CREATE TABLE tip CLONE "PROJECT_UDACITY"."ODS".tip;

CREATE TABLE user CLONE "PROJECT_UDACITY"."ODS".user ;

CREATE TABLE review CLONE "PROJECT_UDACITY"."ODS".review;

CREATE TABLE business CLONE "PROJECT_UDACITY"."ODS".business ;

CREATE TABLE checkin CLONE "PROJECT_UDACITY"."ODS".checkin ;

CREATE TABLE precipitation CLONE "PROJECT_UDACITY"."ODS".precipitation;

CREATE TABLE temperature CLONE "PROJECT_UDACITY"."ODS".temperature;

CREATE TABLE total_info CLONE "PROJECT_UDACITY"."ODS".total_info;

/*SQL queries code that report business name, temperature, precipitation, ratings*/

SELECT name_business, min_t, max_t, normal_min, normal_max, precipitation, precipitation_normal, r.stars
FROM business AS b
JOIN total_info AS tot 
ON b.business_id_main = tot.business_id_main
JOIN temperature AS t
ON t.date_t = tot.date_t
JOIN precipitation AS p
ON p.date_p = tot.date_p
JOIN review AS r
ON r.review_id = tot.review_id;



