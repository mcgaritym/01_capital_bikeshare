Introduction:

The purpose of this project is to a.) create a ride report ETL pipeline and b.) use data science tools to analyze Capital Bikeshare usage around 
Washington, DC before and during the COVID-19 ('coronavirus') pandemic. Questions that come to mind include:

- What is the total number of rides since inception? Since COVID-19?
- What are the highest (peak) and lowest (bottom) demand days/months since 2018? 
- How has ride demand changed before vs. during COVID-19?
- Is the average ride duration shorter, longer, or the same, compared with before COVID-19?
- What bikeshare locations have the greatest difference in rides before/during COVID-19? Is there a trend/clustering to these locations? 
- Can forecasting models predict future demand during COVID-19 and after?
- Can a data pipeline be created for performing ETL operations on ride date and emailing the results?

This brief report is separated into 5 parts; Data Input, Data Cleaning/Processing, Data Analysis, Data Forecasting, and Conclusions. See readme file on github (https://github.com/mcgaritym/capital_bikeshare) for additional details.

What is Capital Bikeshare? From the company (https://www.capitalbikeshare.com/how-it-works):
>Capital Bikeshare is metro DC's bikeshare service, with 4,500 bikes and 500+ stations across 7 jurisdictions: Washington, DC.; Arlington, VA; Alexandria, VA; Montgomery, MD; Prince George's County, MD; Fairfax County, VA; and the City of Falls Church, VA. Designed for quick trips with convenience in mind, it’s a fun and affordable way to get around.

*Note: The pandemic begin date for this analysis is considered to be **1  Mar 2020**, since that is close to when most cities/towns started enacting protective measures. Since it is ongoing the end date is TBD, however, data is only available through 30 Nov 2021 at this time.*

TABLE OF CONTENTS:

0. Introduction
1. Data Input & Cleaning
    * 1.1. Import Libraries
    * 1.2. Create SQL Database Connections and Table
    * 1.3. Clean CSV Files & Load to SQL
    * 1.4. Load SQL Data to DataFrame & Clean
2. Data Analysis
    * 2.1. Compare Total Rides Over Time
    * 2.2. Compare Total Rides By Time of Day
    * 2.3. Compare Total Rides By Day, Year
    * 2.4. Most Popular Ride Stations
    * 2.5. Compare Ride Duration Before/After Pandemic
    * 2.6. Compare Rides by Pandemic Period By Station    
3. Data Predicting/Forecasting
    * 3.1. Demand Forecasting with Linear Regression
    * 3.2. Demand Forecasting with ARIMA
    * 3.3. Demand Forecasting with Auto ARIMA
    * 3.4. Demand Forecasting with Prophet
    * 3.5. Compare all Models
4. Data Pipeline Using Docker, Airflow, AWS S3, AWS RDS
    * 4.0. Summary
    * 4.1. Dockerfile and Docker Compose
    * 4.2. Rideshare DAG
    * 4.3. AWSConnect Class
    * 4.4. Create Database
    * 4.5. Extract Rides & Stations to S3
    * 4.6. Transform, Load Rides & Stations to RDS
    * 4.7. Query Rides
    * 4.8. Email Results
5. Conclusion
