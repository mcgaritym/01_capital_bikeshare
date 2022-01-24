# # load required libraries
# from sqlalchemy import create_engine
# import os
# from glob import glob
# import pandas as pd
# from datetime import timedelta, date, datetime
# from config import *
#
# # get stock tickers, save to SQL
# def get_tickers():
#
#     # get current parent directory and data folder path
#     par_directory = os.path.dirname(os.getcwd())
#     print('Parent Directory: ', par_directory)
#     data_directory = os.path.join(par_directory, 'data')
#     print('Data Directory: ', data_directory)
#     cwd = os.getcwd()
#     print('Current Working Directory: ', cwd)
#     print('Current Files in WD_: ', os.listdir(cwd))
#
#     # retrieve tripdata files
#     tickers = glob(os.path.join(cwd, '*nasdaq_screener*.csv'))
#     print(tickers)
#
#     df_tickers = pd.read_csv(tickers[0])
#     print(df_tickers)
#
#     #  df_tickers = df_tickers[['Symbol', 'Name', 'Market Cap', 'Sector', 'Industry']]
#     df_tickers = df_tickers.dropna(subset=['Market Cap'])
#     df_tickers['Market Cap'] = df_tickers['Market Cap'].astype(str)
#     df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('$', ''))
#     df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('B', '0000000'))
#     df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('M', '0000'))
#     df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('.', ''))
#     df_tickers['Market Cap'] = df_tickers['Market Cap'].astype(int)
#     df_tickers['Market Cap'] = df_tickers['Market Cap'].sort_values(ascending=False)
#     df_tickers['Last Sale'] = df_tickers['Last Sale'].apply(lambda x: x.replace('$', ''))
#     df_tickers['Last Sale'] = df_tickers['Last Sale'].astype(float)
#     df_tickers = df_tickers.sort_values(by='Market Cap', ascending=False)
#
#     # specify second MySQL database connection (faster read_sql query feature)
#     connection_2 = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=MYSQL_USER,
#                                                                     password=MYSQL_ROOT_PASSWORD, host=MYSQL_HOST,
#                                                                     port=MYSQL_PORT, db=MYSQL_DATABASE))
#
#     # send to SQL
#     df_tickers.to_sql(name='tickers', con=connection_2, if_exists="replace", chunksize=1000)
#     df_tickers.to_csv('tickers_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)
#     print(df_tickers.head())
#
#     return "Tickers Created and Sent to SQL"
