#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 08:29:20 2020

@author: mcgaritym
"""
# PART 1: LOAD DATA

#load required libraries
import requests
import io
from io import BytesIO
from bs4 import BeautifulSoup
import urllib.request
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
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
from matplotlib import pyplot as plt
import matplotlib as mpl
import folium
from folium import plugins
from folium.plugins import HeatMap
import numpy as np

files = glob.glob('20*')
files.sort()
#print(files)
df = pd.DataFrame()
for f in files:
    data = pd.read_csv(f)
    df = pd.concat([df, data], axis=0, sort='False')
    
df.reindex()

df['start_station_name'].update(df.pop('Start station'))
df['end_station_name'].update(df.pop('End station'))
df['started_at'].update(df.pop('Start date'))
df['ended_at'].update(df.pop('End date'))
df['member_casual'].update(df.pop('Member type'))
df['started_at'] = pd.to_datetime(df['started_at'])
df['ended_at'] = pd.to_datetime(df['ended_at'])
df['duration'] = df['ended_at'] - df['started_at']
df['number_rides'] = 1
df = df.drop(columns = ['Duration', 'is_equity', 'rideable_type', 'Bike number', 'start_station_id', 'end_station_id'])

df[['start_lat', 'end_lat', 'start_lng', 'end_lng', 'Start station number', 'End station number']] = df[['start_lat', 'end_lat', 'start_lng', 'end_lng', 'Start station number', 'End station number']].apply(pd.to_numeric)
df[['start_station_name', 'end_station_name']] = df[['start_station_name', 'end_station_name']].apply(pd.Categorical)

# alternate methods
#df['start_lat'] = df.groupby(['start_station_name'], sort=False)['start_lat'].fillna(pd.Series.mode)
#df["value"] = df.groupby("name").transform(lambda x: x.fillna(x.mean()))
#df = df.groupby('end_station_name').fillna(df.mode().iloc[0])
#df.D = df.groupby(['A','B','C'])['D'].apply(lambda x: x.fillna(x.mean()))

# filling in missing start and end station numbers and
df['Start station number'] = df['Start station number'].fillna(df.groupby('start_station_name')['Start station number'].transform('mean'))
df['End station number'] = df['End station number'].fillna(df.groupby('end_station_name')['End station number'].transform('mean'))

df['start_lat'] = df['start_lat'].fillna(df.groupby('Start station number')['start_lat'].transform('mean'))
df['end_lat'] = df['end_lat'].fillna(df.groupby('End station number')['end_lat'].transform('mean'))
df['start_lng'] = df['start_lng'].fillna(df.groupby('Start station number')['start_lng'].transform('mean'))
df['end_lng'] = df['end_lng'].fillna(df.groupby('End station number')['end_lng'].transform('mean'))

# rename columns
df = df.rename(columns={'Start station number': 'start_station_id', 'End station number': 'end_station_id'})

print('Shape of DataFrame before dropNA:', df.shape)

# drop rows with missing values
df = df.dropna(subset=['start_lat', 'end_lat', 'start_lng', 'end_lng', 'start_station_id', 'end_station_id', 'start_station_name', 'end_station_name'])

print('Shape of DataFrame after dropNA:', df.shape)

# print sample of columns
df1 = df.iloc[:1000]
df2 = df.iloc[-1000:]

# print(df.groupby('start_station_name')['number_rides'].count().nlargest(10))
# print(df.groupby('start_station_name')['number_rides'].count().nsmallest(10))


#df.groupby('start_station_name')['number_rides'].count().nlargest(10)


## PLOT 1: Barplot of Top 5 Stations
# sns.set(style="darkgrid")
# ax = sns.countplot(x="start_station_name", data=df, palette="Blues_d", order=df.start_station_name.value_counts().iloc[:5].index)
# ax.set(xlabel='ride stations', ylabel='number of rides', title = 'Top 5 ride stations in DC (2018-Present)')
# ax.set_xticklabels(ax.get_xticklabels(), fontsize=8)
# ax.set_xticklabels([textwrap.fill(t.get_text(), 11)  for t in ax.get_xticklabels()], fontsize=8)
#plt.savefig('top5_ride_stations_.png', dpi=300, bbox_inches='tight')



# ## PLOT 2: Barplot of Top 5 Stations During Virus 
# df_covid = df[(df['started_at'] >= '2020-03-01')]
# sns.set(style="darkgrid")
# ax = sns.countplot(x="start_station_name", data=df_covid, palette="Blues_d", order=df_covid.start_station_name.value_counts().iloc[:5].index)
# ax.set(xlabel='ride stations', ylabel='number of rides', title = 'Top 5 ride stations in DC during COVID-19 (March 2020-Present)')
# ax.set_xticklabels(ax.get_xticklabels(), fontsize=10)
# ax.set_xticklabels([textwrap.fill(t.get_text(), 11)  for t in ax.get_xticklabels()], fontsize=10)
# #plt.savefig('top5_ride_stations_.png', dpi=300, bbox_inches='tight')


## PLOT 3: Heatmap of Rides
#df = df.dropna(subset=['start_lat', 'start_lng'])
#m = folium.Map([38.8977, -77.0365], zoom_start=14)

# def generateBaseMap(default_location=[38.8977, -77.0365], default_zoom_start=12):
#     base_map = folium.Map(location=default_location, control_scale=True, zoom_start=default_zoom_start)
#     return base_map
# base_map = generateBaseMap()
# m = HeatMap(data=df[['start_lat', 'start_lng', 'number_rides']].groupby(['start_lat', 'start_lng']).sum().reset_index().values.tolist(), radius=8, max_zoom=12).add_to(base_map)
# m.save('map2.html')


## PLOT 4: Time Based (Start Time) Heatmap of Rides

# base_map = folium.Map(location=[38.8977, -77.0365], control_scale=True, zoom_start=12)

# df['start_lat'] = df['start_lat'].astype(float)
# df['start_lng'] = df['start_lng'].astype(float)

# # filter for 2020 year only
# heat_df = df[(df['started_at'] >= '2019-01-01') & (df['started_at'] <= '2020-01-01') ]

# #create weight column using date
# heat_df['started_at'] = heat_df['started_at'].astype(str)
# heat_df['weight'] = heat_df['started_at'].apply(lambda x: x[5:7])
# heat_df['weight'] = heat_df['weight'].astype(float)

# # reduce dataframe to just lat, long
# heat_df = heat_df[['start_lat', 'start_lng', 'weight']]
# heat_df = heat_df.dropna(axis=0, subset=['start_lat', 'start_lng', 'weight'])

# # list comprehension to make list of lists
# heat_data = [[[row['start_lat'],row['start_lng']] for index, row in heat_df[heat_df['weight'] == i].iterrows()] for i in range(0,13)]

# # plot map
# hm = plugins.HeatMapWithTime(heat_data,auto_play=True,max_opacity=0.8)
# hm.add_to(base_map)

# # display and save map
# base_map
# base_map.save('map5.html')

# Compare Mar 2020 - Jun 2020 compared to 2018 and 2020 rides
df = df[(df['started_at'].dt.month >= 3) & (df['started_at'].dt.month <= 6)]
df['year'] = df['started_at'].apply(lambda x: x.year)
df['year'] = df['year'].astype('category')
df['start_station_name'] = df['start_station_name'].astype('category')


# print sample of columns
df1 = df.iloc[:1000]
df2 = df.iloc[-1000:]

print(df.info())

df_grouped = df.groupby(['start_station_name', 'year'])['number_rides'].count().unstack()
df_grouped.columns = df_grouped.columns.tolist()



print(df_grouped)

print(df_grouped.info())

print(df_grouped.columns)

#df_table = pd.pivot_table(df, values='number_rides', index='start_station_name',columns='year', aggfunc=np.sum)

# print(df_table.index)
# print(df_table.info())
# print(df_table.columns)

# df_table.reset_index()

# print(df_table.index)
# print(df_table.info())
# print(df_table.columns)

# #df_table[['2018', '2019', '2020']] = df_table[['2018', '2019', '2020']].astype(int)
#df_grouped['% Change 2019-2020'] = (df_grouped['2020'] - df_grouped['2019'])/df_grouped['2019'] * 100

# #df_table['% Change 2019-2020'] = df[df['2019', '2020']].pct_change(axis='columns')
# df_table['% Change 2019-2020'].sort_values(ascending=False)



# print(df_table)
# # print(df.groupby('start_station_name')['number_rides'].count().nlargest(10))


