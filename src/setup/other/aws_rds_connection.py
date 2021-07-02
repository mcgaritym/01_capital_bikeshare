#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 21 13:58:11 2021

@author: mcgaritym
"""

import pandas as pd
import pymysql
from sqlalchemy import create_engine

host="bikeshare-database-1.cj4j9csypvlc.us-east-2.rds.amazonaws.com"
port=int(3306)
user="admin"
pwd="Nalgene09!"
database="bikeshare-database-1"

engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{database}",
    echo=False)


stockshistoricdata.to_sql(name=”hisse”, con=mydb, if_exists = ‘replace’, index=False)