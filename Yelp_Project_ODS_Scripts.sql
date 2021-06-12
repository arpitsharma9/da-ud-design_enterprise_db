
/** load yelp and climate files in ODS **/
use database Yelp_Reviews_DB;
use schema ODS;

--create sequences to be used

create or replace sequence seq_tip_id;
create or replace sequence seq_covid_id;
create or replace sequence seq_checkin_id;

--- Creating ODS tables for Climate Data

//temperature ODS
CREATE OR REPLACE TABLE TEMPERATURE (
date_t DATE,
min_t number,
max_t number,
normal_min float,
normal_max float,
constraint pk_date_t primary key(date_t)
);

//precipitation ODS
CREATE OR REPLACE TABLE PRECIPITATION (
date_p date,
precipitation float,
precipitation_normal float,
constraint pk_date_p primary key(date_p)
);


--- Creating ODS tables for Yelp Data

/** inserting climate data into ODS tables **/

insert into TEMPERATURE (
DATE_T,
MIN_T,
MAX_T,
NORMAL_MIN,
NORMAL_MAX)
SELECT
TRY_TO_DATE(DATE_T,'YYYYMMDD'),
TRY_TO_NUMBER(MIN_T),
TRY_TO_NUMBER(MAX_T),
TRY_CAST(NORMAL_MIN AS FLOAT),
TRY_CAST(NORMAL_MAX AS FLOAT)
FROM YELP_REVIEWS_DB.STAGING.TEMPERATURE;

select * from TEMPERATURE;

insert into precipitation (
date_p,
precipitation,
precipitation_normal)
SELECT
TO_DATE(date_p, 'YYYYMMDD'),
TRY_CAST(precipitation as float),
TRY_CAST(precipitation_normal AS FLOAT)
FROM YELP_REVIEWS_DB.STAGING.precipitation;

select * from precipitation;


--- Create users table , followed by insert statement

create or replace table users(
  user_id String,
  name String,
  review_count Number,
  yelping_since Date,
  average_stars float,
  useful Number,
  cool Number,
  elite variant,
  fans Number,
  funny Number,
  friends variant,
  compliment_cool Number,
  compliment_cute Number,
  compliment_funny Number,
  compliment_hot Number,
  compliment_list Number,
  compliment_more Number,
  compliment_note Number,
  compliment_photos Number,
  compliment_plain Number,
  compliment_profile Number,
  compliment_writer Number, 
  constraint pk_user_id primary key(user_id)  
)
  
  
insert into users
select
parse_json($1):  user_id,
parse_json($1):  name,
parse_json($1):  review_count,
parse_json($1):  yelping_since,
parse_json($1):  average_stars,
parse_json($1):  useful,
parse_json($1):  cool,
parse_json($1):  elite,
parse_json($1):  fans,
parse_json($1):  funny,
parse_json($1):  friends,
parse_json($1):  compliment_cool,
parse_json($1):  compliment_cute,
parse_json($1):  compliment_funny,
parse_json($1):  compliment_hot,
parse_json($1):  compliment_list,
parse_json($1):  compliment_more,
parse_json($1):  compliment_note,
parse_json($1):  compliment_photos,
parse_json($1):  compliment_plain,
parse_json($1):  compliment_profile,
parse_json($1):  compliment_writer
from YELP_REVIEWS_DB.staging.users;

--select * from users order by review_count desc;

--- Create business table , followed by insert statement

create or replace table business(
business_id string,
name string,
review_count number,
stars float,
is_open number,
address string,
postal_code string,
city string,
state string,
latitude float,
longitude float,
attributes variant,
categories variant,
hours variant,
constraint pk_business_id primary key(business_id)
);
insert into business
select
parse_json($1):business_id, 
parse_json($1):name, 
parse_json($1):review_count, 
parse_json($1):stars,
parse_json($1):is_open, 
parse_json($1):address, 
parse_json($1):city,
parse_json($1):postal_code, 
parse_json($1):state, 
parse_json($1):latitude, 
parse_json($1):longitude,
parse_json($1):attributes, 
parse_json($1):categories, 
parse_json($1):hours
from YELP_REVIEWS_DB.staging.business;

--select * from business order by REVIEW_COUNT desc;

--- Create TIP table , followed by insert statement

  create or replace table tip (
  tip_id number default seq_tip_id.nextval,
  business_id string,
  compliment_count Number,
  date date,
  text string,
  user_id string,
  constraint pk_tip_id primary key(tip_id),
  CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(user_id),
  CONSTRAINT fk_business_id FOREIGN KEY (business_id) REFERENCES business(business_id)
   );

   insert into tip
   
   select 
   seq_tip_id.nextval,
   parse_json($1):business_id, 
   parse_json($1):compliment_count,
   parse_json($1):date,
   parse_json($1):text, 
   parse_json($1):user_id
   from YELP_REVIEWS_DB.staging.tip;
    
 --  select * from tip order by compliment_count desc;

--- Create checkin table , followed by insert statement

create or replace table checkin (
  checkin_id number default seq_checkin_id.nextval,
  business_id string , 
  date timestamp,
  constraint pk_checkin_id primary key(checkin_id),
  CONSTRAINT fk_business_id FOREIGN KEY (business_id) REFERENCES business(business_id)

);    
  
INSERT INTO checkin 
select 
seq_checkin_id.nextval,
jsonvar:business_id::string ,
y.value::timestamp FROM YELP_REVIEWS_DB.staging.checkin, 
LATERAL FLATTEN(INPUT=>SPLIT(jsonvar:date,',')) y;

--select * from checkin order by checkin_id;


--- Create checkin Covid_Feature , followed by insert statement
  
create or replace table covid_features (
covid_id number default seq_covid_id.nextval,
business_id String,
 virtual_services_offered  String,
 delivery_or_takeout Boolean,
temporary_closed_until String,
call_to_action_enabled Boolean,
request_a_quote Boolean,
grubhub_enabled Boolean,
covid_banner String,
highlights String,
 constraint pk_covid_id primary key(covid_id),
 CONSTRAINT fk_business_id FOREIGN KEY (business_id) REFERENCES business(business_id)
)  
   

   INSERT INTO covid_features 
   select 
   seq_covid_id.nextval,
   parse_json($1):business_id, 
   parse_json($1):"Virtual Services Offered",
   parse_json($1):"delivery or takeout",
   parse_json($1):"Temporary Closed Until",
   parse_json($1):"Call To Action enabled",
   parse_json($1):"Request a Quote Enabled" ,
   parse_json($1):"Grubhub enabled",
   parse_json($1):"Covid Banner",
   parse_json($1): "highlights"
   from YELP_REVIEWS_DB.staging.covid_features;
   
--- Create checkin review , followed by insert statement

create or replace table reviews(
review_id string,
business_id string,
user_id String,
review_date DATE,
stars Number,
funny Number,
cool Number,
useful Number,
review_text String,
constraint pk_review_id primary key(review_id),
CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(user_id),
CONSTRAINT fk_business_id FOREIGN KEY (business_id) REFERENCES business(business_id),
CONSTRAINT fk_date_t FOREIGN KEY (review_date) REFERENCES temperature(date_t),
CONSTRAINT fk_date_p FOREIGN KEY (review_date) REFERENCES precipitation(date_p)
)
insert into reviews
select
parse_json($1):business_id, 
parse_json($1):review_id, 
parse_json($1):user_id, 
parse_json($1):date, 
parse_json($1):stars, 
parse_json($1):funny, 
parse_json($1):cool, 
parse_json($1):useful, 
parse_json($1):text
from YELP_REVIEWS_DB.staging.review;

