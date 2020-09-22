#!/usr/bin/env python
# coding: utf-8

# ## 1. Introduction:
# 
# The purpose of this project is to use data science tools to analyze Capital Bikeshare usage around 
# Washington, DC before and during the COVID-19 ('coronavirus') pandemic to answer questions. Questions that come to mind include:
# 
# - What is the total number of rides since 2018?
# - What are the highest (peak) and lowest (bottom) demand days/months since 2018? 
# - How has total bike shared usage changed before/during COVID-19?
# - Is the average ride duration shorter, longer, or the same, compared with before COVID-19?
# - What bikeshare locations have the greatest difference in rides before/during COVID-19? Is there a trend/clustering to these locations? 
# 
# This brief report is separated into 5 parts; Intro, Data Input, Data Cleaning/Processing, Data Analysis, Conclusions. See readme file on github [here] (https://github.com/mcgaritym/capital_bikeshare) for additional details.
# 
# What is Capital Bikeshare? From the company [site](https://www.capitalbikeshare.com/how-it-works):
# >Capital Bikeshare is metro DC's bikeshare service, with 4,500 bikes and 500+ stations across 7 jurisdictions: Washington, DC.; Arlington, VA; Alexandria, VA; Montgomery, MD; Prince George's County, MD; Fairfax County, VA; and the City of Falls Church, VA. Designed for quick trips with convenience in mind, it’s a fun and affordable way to get around.
# 
# *Note: The pandemic begin date for this analysis is considered to be **1  Mar 2020**, since that is close to when most cities/towns started enacting protective measures. Since it is ongoing the end date is TBD, however, data is only available through 30 Jun 2020 at this time...*

# ## 2. Data Input:
# 
# Data is provided by Capital Bikeshare [here](https://www.capitalbikeshare.com/system-data) on a monthly basis and provides data on all rides. Data was loaded for all 2018, 2019, and 2020 (through June) months. 
# 

# In[87]:


# load required libraries
import requests
import io
from io import BytesIO
from bs4 import BeautifulSoup
import urllib.request
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import datetime
from datetime import date, datetime
import re
import time
from urllib.request import urlopen
import zipfile
from zipfile import ZipFile
import glob
import os
from urllib.request import urlopen, Request
import seaborn as sns
import textwrap
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.cm as cm
import calendar
import matplotlib.dates as mdates
import folium
from folium import plugins
from folium.plugins import HeatMap
import numpy as np
import holoviews as hv
from holoviews import opts
hv.extension('bokeh')

# gather files in local directory, create empty dataframe, and concatenate data
files = glob.glob('20*')
files.sort()
df = pd.DataFrame()
for f in files:
    data = pd.read_csv(f)
    df = pd.concat([df, data], axis=0, sort='False')

# reindex data 
df = df.reindex()


# ## 3. Data Cleaning/Processing:

# In[88]:


# fill columns (different names) with same values
df.reset_index(level=0, inplace=True)
df['start_station_name'].update(df.pop('Start station'))
df['end_station_name'].update(df.pop('End station'))
df['started_at'].update(df.pop('Start date'))
df['ended_at'].update(df.pop('End date'))
df['member_casual'].update(df.pop('Member type'))

# drop unnecessary, redundant or blank columns
df = df.drop(columns = ['Duration', 'is_equity', 'rideable_type', 'Bike number', 'start_station_id', 'end_station_id'])

# convert to numeric, categorical or datetime data types
df[['start_lat', 'end_lat', 'start_lng', 'end_lng', 'Start station number', 'End station number']] = df[['start_lat', 'end_lat', 'start_lng', 'end_lng', 'Start station number', 'End station number']].apply(pd.to_numeric)
df[['start_station_name', 'end_station_name']] = df[['start_station_name', 'end_station_name']].apply(pd.Categorical)
df['started_at'] = pd.to_datetime(df['started_at'])
df['ended_at'] = pd.to_datetime(df['ended_at'])

# create new duration and number of rides column
df['duration'] = df['ended_at'] - df['started_at']
df['number_rides'] = 1

# fill in missing start and end station numbers based on other rows (same station name) that have start and end station numbers
df['Start station number'] = df['Start station number'].fillna(df.groupby('start_station_name')['Start station number'].transform('mean'))
df['End station number'] = df['End station number'].fillna(df.groupby('end_station_name')['End station number'].transform('mean'))

# fill in missing latitude/longitude data based on other rows (same station number) that have latitude/longitude numbers
df['start_lat'] = df['start_lat'].fillna(df.groupby('Start station number')['start_lat'].transform('mean'))
df['end_lat'] = df['end_lat'].fillna(df.groupby('End station number')['end_lat'].transform('mean'))
df['start_lng'] = df['start_lng'].fillna(df.groupby('Start station number')['start_lng'].transform('mean'))
df['end_lng'] = df['end_lng'].fillna(df.groupby('End station number')['end_lng'].transform('mean'))

# rename columns
df = df.rename(columns={'Start station number': 'start_station_id', 'End station number': 'end_station_id'})

# print sample of data
print('\n Data Sample: \n')
df.head()


# ## 4. Data Analysis
# 

# ### 4.1 Data Analysis - Total Rides Over Time
# 
# The first plot below shows total rides over time until present. The red shaded region indicates the COVID-19 pandemic period of time. 

# In[89]:


# plot number of rides over time (weekly)
#df.reset_index(level=0, inplace=True)
df['started_at'] = pd.to_datetime(df['started_at'])
df = df.set_index(['started_at'])
df_weekly = df['number_rides'].resample('W').sum()

# set plot options (lineplot, size, axis labels, etc)
plt.figure(figsize = (12, 8), dpi=300)
#plt.figure(dpi=200)
sns.lineplot(x=df_weekly.index, y=df_weekly[:])
plt.title('Capital Bikeshare Rides by Week (2018-Present)', fontsize=16, fontweight='bold')
plt.ylabel('Number of Rides', fontsize=14)
plt.ylim((0,150000))
plt.xlabel('Dates (2018-Present)', fontsize=14)
plt.xticks(rotation=45, fontsize=12) 
plt.axvspan(datetime(2020,3,1), datetime(2020,7,30), color='red', alpha=0.2)
plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
sns.set_style("whitegrid")
plt.show()


# ### 4.2 Data Analysis - Compare Total Rides By Time of Day
# 
# The second plot is a timewheel (radial) heatmap that shows which hours of the day and which days of the week experience the highest number of rides. Each slice represents a time (hour of day) while each layer represents a day of the week.
# 

# In[90]:


# create new dataframe for timewheel/heatmap plot (number of rides per hour per day of week)
yticks = ['Sun', 'Sat', 'Fri', 'Thu', 'Wed', 'Tue', 'Mon']
#xticks = ['0:00', '1:00', '2:00', '3:00', '4:00', '5:00', '6:00', '7:00', '8:00', '9:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00', '24:00']
opts.defaults(opts.HeatMap(cmap= 'PuBuGn', radial=True, width=600, height=600, yticks=yticks, xticks=24, tools=['hover'], colorbar=True))
df_hourly = df['number_rides'].resample('H').sum()
df_hourly = df_hourly.reset_index()
df_hourly.columns = ['started_at', 'number_rides']
df_hourly['hour'] = df_hourly['started_at'].apply(lambda x: x.strftime('%H:%M'))
df_hourly['day'] = df_hourly['started_at'].apply(lambda x: x.strftime('%A'))

# create heatmap object based on new dataframe 
#df_heatmap = pd.DataFrame({"values": df_hourly['number_rides'], "hour": df_hourly['hour'], "day": df_hourly['day']}, index=df_hourly['started_at'])
heatmap = hv.HeatMap(df_hourly, ['hour', 'day'], ['number_rides'], label='Timewheel Heatmap of Average Rides Per Hour, Day of Week').opts(fontsize={'title': 14})
heatmap
redimensioned_heatmap = heatmap.redim.range(number_rides=(0, 800))
redimensioned_heatmap


# The above figure shows that 0700-0900 and 1600-1800 are the most popular times for bikeshare in an average week. In addition, the middle weekdays (Tue-Wed) are the most popular days for bikeshare. Interestingly, when averaged across all stations, the weekend days (Sat, Sun) are not significantly high compared to the weekdays. This may be due to the large volume of commuters/rides using bikeshare for work, which outpaces the riders using bikeshare on the weekend (likely more for leisure, not work). 

# ### 4.3 Data Analysis - Compare Total Rides By Year
# 

# In[93]:


# compare total rides, total rides by year (Note: 2020 data until Jun)
df = df.reset_index()
df['year'] = df['started_at'].apply(lambda x: x.year)
df['month'] = df['started_at'].apply(lambda x: x.month)
print('\n Total number of rides (2018-Present): \n', len(df))
print('\n Total number of stations (2018-Present): \n', df['start_station_name'].nunique())
print('\n Three highest ride days (2018-Present): \n\n', df.set_index(['started_at'])['number_rides'].resample('D').sum().nlargest(3))
print('\n Total number of rides (2018-Present) By Year: \n\n', df.groupby('year')['number_rides'].sum())


# ### 4.4 Data Analysis - Most Popular Ride Stations
# 

# In[94]:


# find top 15 most popular ride stations since 2018:
df_stations = df.groupby('start_station_name')['number_rides'].count().nlargest(50)
df_stations = df_stations.reset_index().sort_values('number_rides', ascending=False)
df_stations['start_station_name'] = df_stations['start_station_name'].astype('object')
df_stations['number_rides'] = df_stations['number_rides'].astype('float')

# plot top 50 stations in horizontal bar plot

f, ax = plt.subplots(figsize=(8, 12), dpi=200)
#plt.figure(dpi=300)
#sns.set(style="whitegrid")
sns.set_color_codes("muted")
ax = sns.barplot(x="number_rides", y="start_station_name", 
            data=df_stations, 
            orient = "h")
ax.set_xlabel('Number of Rides', fontsize=12)
ax.set_ylabel('Ride Stations', fontsize=12)
#ax.set_ylabel('Ride Stations', fontsize=12)
ax = ax.set_title('Number of Rides per Station (2018-Present)', fontsize=18, fontweight='bold')
# #print('\n Most popular stations (highest # of rides) since 2018: \n', df.groupby('start_station_name')['number_rides'].count().nlargest(15))


# In the above figure, Columbus Circle/Union Station (approx. 130k) and Lincoln Memorial (approx. 100k) clearly experience the most rides in comparison with other stations. The remaining stations show a gradual decrease in number of rides.

# ### 4.5 Data Analysis - Compare Rides By Pandemic Period (Mar-Jun) By Year
# 

# In[96]:


# compare Mar-Jun period of 2020 (total) versus 2018, 2019
#df = df.reset_index()
df_grouped = df[(df['started_at'].dt.month >= 3) & (df['started_at'].dt.month <= 6)]
print('\n Total number of rides (Mar-Jun) By Year: \n\n', df_grouped.groupby('year')['number_rides'].sum())

# setting ignore for SettingWithCopy warning
pd.options.mode.chained_assignment = None

# convert duration to seconds, and seconds to minutes
df_grouped['duration'] = df_grouped['duration'].apply(lambda x: x.total_seconds() * (1/60))

# display average ride duration by year
print('\n Average ride time in Minutes (Mar-Jun) By Year: \n\n', df_grouped.groupby('year')['duration'].mean())


# Interestingly, the above data shows that, even though the number of rides has significantly decreased (1.29 mil to 601k) for the Mar-Jun time period, the average length of ride time has significantly increased (20 mins to 35 mins). Perhaps riders are taking longer, leisurely bike rides outside due to the COVID-19 lockdown and staying inside. Or perhaps it is related to the increase in telework, and therefore there are less rides to/from work (e.g. from a metro station to the office), which could be shorter in duration. 

# ### 4.6 Data Analysis - Compare Rides By Pandemic Period (Mar-Jun) By Station
# 

# In[97]:


# Compare Mar-Jun period of 2020 (by station) versus 2018, 2019
df_grouped['start_station_name'] = df_grouped['start_station_name'].astype('category')
df_grouped_2 = df_grouped.groupby(['start_station_name', 'year'])['number_rides'].count().unstack()

df_grouped_2.columns = df_grouped_2.columns.astype(list)
df_grouped_2.columns = ['2018', '2019', '2020']
df_grouped_2 = df_grouped_2[(df_grouped_2['2019'] != 0) & (df_grouped_2['2020'] != 0)]

df_grouped_2['% Change 2019-2020'] = (df_grouped_2['2020'] - df_grouped_2['2019'])/df_grouped_2['2019'] * 100

print('\n Top Stations of Ride Growth (Increase): \n\n', df_grouped_2.sort_values(by ='% Change 2019-2020' , ascending=False).iloc[:5])
print('\n Top Stations of Ride Loss (Decrease): \n\n', df_grouped_2.sort_values(by ='% Change 2019-2020' , ascending=True).iloc[:5])


# In[99]:


# fill in long, lat from previous dataframe via map function
df_grouped_2 = df_grouped_2.reset_index()
mapping_lat = dict(df_grouped[['start_station_name', 'start_lat']].values)
df_grouped_2['start_lat'] = df_grouped_2['start_station_name'].map(mapping_lat)
mapping_lng = dict(df_grouped[['start_station_name', 'start_lng']].values)
df_grouped_2['start_lng'] = df_grouped_2['start_station_name'].map(mapping_lng)

# # create increase/decrease column for map markers
df_grouped_2['ride_change'] = ["Increase" if i >=0 else "Decrease" if i <=0 else i for i in df_grouped_2['% Change 2019-2020']]

# create base of Washington DC
base_map = folium.Map([38.8977, -77.0365], zoom_start=11)

# add map layer of stations with most ride growth, ride loss as green/red markers 
ride_increase_decrease = folium.map.FeatureGroup()
latitudes = list(df_grouped_2.start_lat)
longitudes = list(df_grouped_2.start_lng)
labels = list(df_grouped_2.ride_change)
for lat, lng, label in zip(latitudes, longitudes, labels):
  if label == 'Decrease':
    folium.Marker(
      location = [lat, lng], 
      popup = label,
      icon = folium.Icon(color='red', icon='info-sign')
     ).add_to(base_map) 
  else:
    folium.Marker(
      location = [lat, lng], 
      popup = label,
      icon = folium.Icon(color='green', icon='info-sign')
     ).add_to(base_map)
base_map.add_child(ride_increase_decrease)

# add title
title_html = '''
     <h3 align="center" style="font-size:20px"><b>Change in Rides by Station (Mar-Jun 2019 vs. Mar-Jun 2020)</b></h3>
     ''' 
base_map.get_root().html.add_child(folium.Element(title_html))

# display map
base_map


# In the above figure, stations that experienced a ride decrease are marked in <font color='red'>**red**</font>, while stations that experienced a ride increase are marked in <font color='green'>**green**</font>.
# 
# The following observations can be seen:
#     
# - The majority of stations are <font color='red'>**red**</font>, indicating total ride decrease compared to 2019. This makes sense since previous sections showed a net decrease in total rides compared to earlier years. 
# - Many of the <font color='red'>**red**</font> marked stations are clustered together around central/northern Washington, DC. It also appears as though many of them follow the Metro yellow line (Alexandria), silver/orange line (Arlington) and red line (Bethesda). 
# - Many of the <font color='green'>**green**</font> marked stations are clustered together around four mile run drive/park in Arlington/Alexandria, southern/eastern Washington DC,  and southern Maryland (College Park, Hyattsville).
# 

# ## 5. Conclusion 
# 
# 

# This was a quick and simple data science project for visualizing bikeshare patterns across DC before and after COVID 19. The conclusions/takeaways for each section can be found in the text beneath each figure/plot/data print. In future projects, my hope is to use more advanced data science tools to apply machine learning to data sets. 

# In[ ]:




