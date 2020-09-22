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

## set driver options, request options, and url
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--headless')
options.add_argument('--incognito')
driver = webdriver.Chrome(ChromeDriverManager().install())
headers = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.39 Safari/537.36', 
            "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
url = 'https://s3.amazonaws.com/capitalbikeshare-data/index.html'
driver.get(url)
time.sleep(2)
driver.execute_script("window.scrollTo(0, 500000)")
time.sleep(2)
page_source = driver.page_source
soup = BeautifulSoup(page_source, "html.parser")

## get links for data and filter for 2018-2020 (current)
links = []
for link in soup.findAll('a'):
    link_text = link.get('href')
    str(link_text)
    if ('2020' in link_text) or ('2019' in link_text) or ('2018' in link_text):
        links.append(link_text)
print(links)

## get data from links, and save zip to disk/folder or convert zip directly to DataFrame
df = pd.DataFrame()
for url in links:

    ## save zip files to disk/folder
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()
    
    ## METHOD 2: dont save zip files to disk/folder
    # remote_zip_file = urlopen(url)
    # zipinmemory = BytesIO(remote_zip_file.read())
    # zip_file = zipfile.ZipFile(zipinmemory)
    # data = pd.read_csv(zip_file.open(zip_file.namelist()[0]))
    # df = pd.concat([df, data], axis=0, sort=False)
    # print(df.tail())

#----------------------------------------------------------------------------
