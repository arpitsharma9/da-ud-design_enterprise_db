/** load data into a STAR schema in DWH"

CREATE SCHEMA DWH;
use database Yelp_Reviews_DB;
use schema DWH;

--- Create DIM_Temperature table and insert data into it --- 

CREATE OR REPLACE TABLE DIM_Temperature (
date_t DATE,
min_t number,
max_t number,
normal_min float,
normal_max float,
constraint pk_date_t primary key(date_t)
);

insert into DIM_Temperature 
select Distinct DATE_T,MIN_T,MAX_T, Normal_MIN,Normal_MAX 
from YELP_REVIEWS_DB.ODS.Temperature;

--- Create DIM_Precipitation table and insert data into it --- 

CREATE OR REPLACE TABLE DIM_PRECIPITATION (
date_p date,
precipitation float,
precipitation_normal float,
constraint pk_date_p primary key(date_p)
);
   
insert into DIM_PRECIPITATION 
select Distinct DATE_P,precipitation,precipitation_normal
from YELP_REVIEWS_DB.ODS.PRECIPITATION ;   
  
    
--- Create DIM_Business table and insert data into it --- 

create or replace table DIM_BUSINESS(
business_id string,
name string,
address string,
postal_code string,
city string,
state string,
constraint pk_business_id primary key(business_id)
);
   
insert into DIM_BUSINESS
select Distinct business_id,name,address,postal_code,city,state
from YELP_REVIEWS_DB.ODS.BUSINESS
 ;   
  
--- Create DIM_Users table and insert data into it ---  
  
create or replace table DIM_USERS(
  user_id String,
  name String,
  yelping_since Date,
  constraint pk_user_id primary key(user_id)  
)  

insert into DIM_USERS
Select distinct user_ID,name,yelping_since 
from YELP_REVIEWS_DB.ODS.users;

Select * from DIM_Temperature;
Select * from DIM_Precipitation;
Select * from DIM_USERs;
Select * from DIM_Business;
 
--- create. a fact table fact_review -----


create or replace table Fact_Review (   
review_id string,
business_id string,
user_id String,
Date_Temperature DATE,
Date_Precipitation DATE,
stars Number,
User_review_Count Number,
Business_Review_count Number,

constraint fk_review_id foreign key (review_id)
references YELP_REVIEWS_DB.ODS.REVIEWS (REVIEW_ID),   
constraint fk_business_id foreign key (business_id)
references YELP_REVIEWS_DB.ODS.BUSINESS  (BUSINESS_ID),   
constraint fk_user_id  foreign key (user_id )
references YELP_REVIEWS_DB.ODS.USERS  (USER_ID),   
constraint fk_Date_Temperature foreign key (Date_Temperature)
references YELP_REVIEWS_DB.ODS.TEMPERATURE (DATE_T),   
constraint fk_Date_Precipitation foreign key (Date_Precipitation)
references YELP_REVIEWS_DB.ODS.PRECIPITATION (DATE_P)
)
 
INSERT INTO Fact_Review
select R.review_id,B.Business_ID,U.User_ID,T.Date_T,P.Date_P,R.stars,U.review_Count,B.review_Count
from YELP_REVIEWS_DB.ODS.REVIEWS as R
JOIN YELP_REVIEWS_DB.ODS.BUSINESS as B
on R.business_ID=B.business_ID
JOIN YELP_REVIEWS_DB.ODS.USERS as U
on R.user_ID=U.user_ID
JOIN YELP_REVIEWS_DB.ODS.TEMPERATURE as T 
on R.review_date=T.Date_T
JOIN YELP_REVIEWS_DB.ODS.PRECIPITATION as P
on R.review_date=P.Date_P;



--- Write a Final Query 

SELECT
    B.NAME as Business_Name,
	T.MIN_T as Min_Temp,
    T.MAX_T as Max_Temp,
    P.precipitation as precipitation,
    P.precipitation_normal as normal_precipitation,
    AVG(R.Stars) as Avg_Rating,
    Count(R.Stars) as Num_Rating

FROM Fact_Review R
JOIN Dim_Temperature as T
ON (R.Date_Temperature = T.DATE_T)
JOIN Dim_Precipitation as  P
ON (R.Date_Precipitation = P.DATE_P)
JOIN Dim_Business B 
ON (R.business_id = B.business_id)
group by B.NAME,T.MIN_T,T.MAX_T,P.precipitation,P.precipitation_normal
order by Num_Rating desc;







-