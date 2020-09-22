#!/usr/bin/env python
# coding: utf-8

# In[43]:


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
import numpy as np


# In[45]:


# gather files in local directory, create empty dataframe, and concatenate data
files = glob.glob('*capitalbikeshare*')
files.sort()
df = pd.DataFrame()
for f in files:
    data = pd.read_csv(f)
    df = pd.concat([df, data], axis=0, sort='False')


# In[46]:


# reindex data 
df.reset_index(inplace=True, drop=True)


# In[47]:


df.head()


# In[48]:


df.tail()


# In[49]:


df.info()


# In[50]:


df.shape


# In[51]:


df.isnull().sum(axis = 0)


# In[52]:


# fill columns (different names) with same values
df['start_station_name'].update(df.pop('Start station'))
df['end_station_name'].update(df.pop('End station'))
df['started_at'].update(df.pop('Start date'))
df['ended_at'].update(df.pop('End date'))
df['member_casual'].update(df.pop('Member type'))


# In[53]:


df.head()


# In[54]:


df.isnull().sum(axis = 0)


# In[55]:


# drop unnecessary, redundant or blank columns
df = df.drop(columns = ['Duration', 'is_equity', 'rideable_type', 'Bike number', 'start_station_id', 'end_station_id'])


# In[56]:


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


# In[57]:


# print sample of data
print('\n Data Sample: \n')
df.head()


# In[90]:


# print sample of data
print('\n Data Sample: \n')
df.tail()


# In[94]:


# resample number of rides weekly
df.reset_index(level=0, inplace=True)
df['started_at'] = pd.to_datetime(df['started_at'])
df = df.set_index(['started_at'])
df_week = df['number_rides'].resample('D').sum()


# In[95]:


# print sample of data
print('\n Data Sample: \n')
df_week.head()


# In[122]:


# print sample of data
print('\n Data Sample: \n')
df_week.tail()


# In[126]:


from datetime import datetime as dt
from datetime import date
#df_week = df_week.set_index('started_at')
df_week['days_from_start'] = (df_week.index - df_week.index[0]).days
#df_week = df_week.reset_index()
df_week.tail()



# In[137]:


from sklearn.model_selection import train_test_split
from sklearn.linear_model import Ridge

X = df_week['days_from_start'].values.reshape(-1,1)
y = df_week['number_rides'].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, shuffle=False, random_state=42)


# In[139]:


# Fit our model and generate predictions
from sklearn.metrics import r2_score

model = Ridge()
model.fit(X_train, y_train)
predictions = model.predict(X_test)
score = r2_score(y_test, predictions)
print(score)


# In[143]:


# Visualize our predictions along with the "true" values, and print the score
fig, ax = plt.subplots(dpi=200)
ax.plot(y_test, color='k', lw=1)
ax.plot(predictions, color='r', lw=2)
plt.show()


# In[ ]:




