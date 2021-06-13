//** Let's create a new database and create statging **/

create or Replace DATABASE Yelp_Reviews_DB COMMENT = 'Project - how weather affects restaurant ratings';
create or replace schema Staging;
create or replace  schema ODS;
create or replace  schema DATAWAREHOUSE;



/** load yelp and climate files in staging area **/
use database Yelp_Reviews_DB;
use schema staging;

--- Create Load file format for JSON data structure 

CREATE OR REPLACE FILE FORMAT JSON_FORMAT TYPE = 'JSON' COMPRESSION = 'AUTO' STRIP_OUTER_ARRAY = TRUE  COMMENT = 'File format for JSON Loading';

CREATE or replace FILE FORMAT CSV_FORMAT TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY ='"' TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N') COMMENT = 'File format for CSV Loading';



-- Create Stage for Yelp and Climate Data
CREATE or REPLACE STAGE Staging_Yelp  COMMENT = 'Staging for Yelp Files ';
CREATE or REPLACE STAGE Staging_Climate_Data  COMMENT = 'Staging for Climate Data Files ';


-- Create staging tables with one column of type variant for Yelp and Climate Datasets.

create or replace table Business(jsonvar variant) ;
create or replace table Checkin(jsonvar variant) ;
create or replace table Review(jsonvar variant) ;
create or replace table Tip(jsonvar variant) ;
create or replace table Users(jsonvar variant) ;
create or replace table Covid_Features(jsonvar variant) ;
CREATE OR REPLACE TABLE PRECIPITATION (
date_p String,
precipitation String,
precipitation_normal String
);
CREATE OR REPLACE TABLE TEMPERATURE (
date_t String,
min_t String,
max_t String,
normal_min String,
normal_max String
);

/** 6 Yelp files have to be loaded from local into  a Snowflake staging schema, using the command-line snowsql tool.
 2 climate file would be loaded from local into Snowflake using browser. i.e Web UI
**/

-- Upload Yelp file to staging area. Use the snowsql tool. 

-- 1. Business 

PUT file:///Users/arpitsharma/Desktop/ML/09_Udacity/06-Udacity_Data_Architect/Project_2/datasets/Yelp/business.json  @Staging_Yelp auto_compress=true;
--list @Staging_Yelp;
--Now you can copy data into the staging table 

COPY INTO Business FROM @Staging_Yelp/business.json.gz FILE_FORMAT = JSON_FORMAT ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
select * from Business;

-- 2. Checkin
PUT file:///Users/arpitsharma/Desktop/ML/09_Udacity/06-Udacity_Data_Architect/Project_2/datasets/Yelp/checkin.json  @Staging_Yelp auto_compress=true;
list @Staging_Yelp;
COPY INTO checkin FROM @Staging_Yelp/checkin.json.gz FILE_FORMAT = JSON_FORMAT ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
select * from checkin;

-- 3. Review
PUT file:///Users/arpitsharma/Desktop/ML/09_Udacity/06-Udacity_Data_Architect/Project_2/datasets/Yelp/Review.json  @Staging_Yelp auto_compress=true;
list @Staging_Yelp;
COPY INTO Review FROM @Staging_Yelp/Review.json.gz FILE_FORMAT = JSON_FORMAT ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
select * from Review limit 10;

--4 TIP
PUT file:///Users/arpitsharma/Desktop/ML/09_Udacity/06-Udacity_Data_Architect/Project_2/datasets/Yelp/TIP.json  @Staging_Yelp auto_compress=true;
list @Staging_Yelp;
COPY INTO TIP FROM @Staging_Yelp/TIP.json.gz FILE_FORMAT = JSON_FORMAT ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
select * from TIP;

-- 5. Users
PUT file:///Users/arpitsharma/Desktop/ML/09_Udacity/06-Udacity_Data_Architect/Project_2/datasets/Yelp/user.json  @Staging_Yelp auto_compress=true;
list @Staging_Yelp;
COPY INTO Users FROM @Staging_Yelp/user.json.gz FILE_FORMAT = JSON_FORMAT ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
select * from Users;

-- 6. Covid Features
PUT file:///Users/arpitsharma/Desktop/ML/09_Udacity/06-Udacity_Data_Architect/Project_2/datasets/Yelp/Covid_features.json  @Staging_Yelp auto_compress=true;
list @Staging_Yelp;
COPY INTO Covid_features FROM @Staging_Yelp/Covid_features.json.gz FILE_FORMAT = JSON_FORMAT ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
select * from Covid_features;


-- Loading these files from UI for precipitation and temperature

-- 7. Temperature
PUT file:///Users/arpitsharma/Desktop/ML/09_Udacity/06-Udacity_Data_Architect/Project_2/datasets/Climate/temperature-degreeF.csv  @Staging_Climate_Data auto_compress=true;
list @Staging_Climate_Data;
COPY INTO temperature FROM @Staging_Climate_Data/temperature-degreeF.csv.gz FILE_FORMAT = CSV_FORMAT ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
select * from Temperature;

-- 8. Precipitation

PUT file:///Users/arpitsharma/Desktop/ML/09_Udacity/06-Udacity_Data_Architect/Project_2/datasets/Climate/precipitation-inch.csv  @Staging_Climate_Data auto_compress=true;
list @Staging_Climate_Data;
COPY INTO precipitation FROM @Staging_Climate_Data/precipitation-inch.csv.gz FILE_FORMAT = CSV_FORMAT ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
select * from precipitation;



