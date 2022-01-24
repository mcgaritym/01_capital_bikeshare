# # load required libraries
# from sqlalchemy import create_engine
# import pandas as pd
# import numpy as np
# import yfinance as yf
# import time
# from config import *
#
# # get ticker financial info via yfinance API:
# def get_financials():
#
#     # specify second MySQL database connection (faster read_sql query feature)
#     connection_2 = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=MYSQL_USER,
#                                                                     password=MYSQL_ROOT_PASSWORD, host=MYSQL_HOST,
#                                                                     port=MYSQL_PORT, db=MYSQL_DATABASE))
#
#     # read SQL table for stocks with positive market cap and remove 2021 IPO stocks
#     tickers_list = pd.read_sql_query(
#         """SELECT Symbol FROM tickers WHERE `Market Cap` > 0 AND `IPO Year` != '2021' OR `IPO Year` IS NULL;""",
#         con=connection_2)
#     connection_2.dispose()
#
#     # only look at first 100 tickers by market cap
#     tickers_list = tickers_list[:20]
#
#     # create empty list to append dictionary values
#     list_financials = []
#     counter = 0
#
#     # loop over ticker symbols
#     for row in tickers_list.values:
#
#         # get symbol
#         counter = counter + 1
#         symbol = row[0]
#         print(symbol, counter)
#
#         # get ticker information
#         try:
#
#             # input various stock tickers
#             ticker = yf.Ticker(symbol)
#             stock_quarterly_earnings = ticker.quarterly_earnings
#             stock_yearly_earnings = ticker.earnings
#             stock_info = ticker.info
#
#         except Exception as e:
#             print(e)
#             pass
#
#         # try to append values to dictionary list
#         try:
#             list_financials.append(dict({'symbol': symbol,
#                                          'earnings-0Q': stock_quarterly_earnings['Earnings'].iloc[-1],
#                                          'earnings-1Q': stock_quarterly_earnings['Earnings'].iloc[-2],
#                                          'earnings-2Q': stock_quarterly_earnings['Earnings'].iloc[-3],
#                                          'earnings-3Q': stock_quarterly_earnings['Earnings'].iloc[-4],
#                                          'earnings-0Y': stock_yearly_earnings['Earnings'].iloc[-1],
#                                          'earnings-1Y': stock_yearly_earnings['Earnings'].iloc[-2],
#                                          'earnings-2Y': stock_yearly_earnings['Earnings'].iloc[-3],
#                                          'revenue-0Q': stock_quarterly_earnings['Revenue'].iloc[-1],
#                                          'revenue-1Q': stock_quarterly_earnings['Revenue'].iloc[-2],
#                                          'revenue-2Q': stock_quarterly_earnings['Revenue'].iloc[-3],
#                                          'revenue-3Q': stock_quarterly_earnings['Revenue'].iloc[-4],
#                                          'revenue-0Y': stock_yearly_earnings['Revenue'].iloc[-1],
#                                          'revenue-1Y': stock_yearly_earnings['Revenue'].iloc[-2],
#                                          'revenue-2Y': stock_yearly_earnings['Revenue'].iloc[-3],
#                                          'trailingPE': stock_info['trailingPE'],
#                                          'trailingEps': stock_info['trailingEps'],
#                                          'priceToSalesTrailing12Months': stock_info['priceToSalesTrailing12Months'],
#                                          'quickRatio': stock_info['quickRatio'],
#                                          'currentRatio': stock_info['currentRatio'],
#                                          'debtToEquity': stock_info['debtToEquity'],
#                                          'profitMargins': stock_info['profitMargins'],
#                                          'pegRatio': stock_info['pegRatio'],
#                                          'twoHundredDayAverage': stock_info['twoHundredDayAverage'],
#                                          'fiftyDayAverage': stock_info['fiftyDayAverage'],
#                                          'fiftyTwoWeekHigh': stock_info['fiftyTwoWeekHigh'],
#                                          'dividendRate': stock_info['dividendRate'],
#                                          'industry': stock_info['industry'],
#                                          'sector': stock_info['sector']}))
#
#             # if exception occurs, print
#         except KeyError as e:
#             print('KeyError: ', e)
#
#             # if KeyError exception is trailingPE, append with trailingPE = nan
#             if 'trailingPE' in str(e):
#
#                 try:
#
#                     list_financials.append(dict({'symbol': symbol,
#                                                  'earnings-0Q': stock_quarterly_earnings['Earnings'].iloc[-1],
#                                                  'earnings-1Q': stock_quarterly_earnings['Earnings'].iloc[-2],
#                                                  'earnings-2Q': stock_quarterly_earnings['Earnings'].iloc[-3],
#                                                  'earnings-3Q': stock_quarterly_earnings['Earnings'].iloc[-4],
#                                                  'earnings-0Y': stock_yearly_earnings['Earnings'].iloc[-1],
#                                                  'earnings-1Y': stock_yearly_earnings['Earnings'].iloc[-2],
#                                                  'earnings-2Y': stock_yearly_earnings['Earnings'].iloc[-3],
#                                                  'revenue-0Q': stock_quarterly_earnings['Revenue'].iloc[-1],
#                                                  'revenue-1Q': stock_quarterly_earnings['Revenue'].iloc[-2],
#                                                  'revenue-2Q': stock_quarterly_earnings['Revenue'].iloc[-3],
#                                                  'revenue-3Q': stock_quarterly_earnings['Revenue'].iloc[-4],
#                                                  'revenue-0Y': stock_yearly_earnings['Revenue'].iloc[-1],
#                                                  'revenue-1Y': stock_yearly_earnings['Revenue'].iloc[-2],
#                                                  'revenue-2Y': stock_yearly_earnings['Revenue'].iloc[-3],
#                                                  'trailingPE': np.nan,
#                                                  'trailingEps': np.nan,
#                                                  'priceToSalesTrailing12Months': stock_info[
#                                                      'priceToSalesTrailing12Months'],
#                                                  'quickRatio': stock_info['quickRatio'],
#                                                  'currentRatio': stock_info['currentRatio'],
#                                                  'debtToEquity': stock_info['debtToEquity'],
#                                                  'profitMargins': stock_info['profitMargins'],
#                                                  'pegRatio': stock_info['pegRatio'],
#                                                  'twoHundredDayAverage': stock_info['twoHundredDayAverage'],
#                                                  'fiftyDayAverage': stock_info['fiftyDayAverage'],
#                                                  'fiftyTwoWeekHigh': stock_info['fiftyTwoWeekHigh'],
#                                                  'dividendRate': stock_info['dividendRate'],
#                                                  'industry': stock_info['industry'],
#                                                  'sector': stock_info['sector']}))
#
#                 except:
#                     pass
#
#             elif 'Earnings' in str(e):
#
#                 try:
#
#                     list_financials.append(dict({'symbol': symbol,
#                                                  'earnings-0Q': np.nan,
#                                                  'earnings-1Q': np.nan,
#                                                  'earnings-2Q': np.nan,
#                                                  'earnings-3Q': np.nan,
#                                                  'earnings-0Y': np.nan,
#                                                  'earnings-1Y': np.nan,
#                                                  'earnings-2Y': np.nan,
#                                                  'revenue-0Q': np.nan,
#                                                  'revenue-1Q': np.nan,
#                                                  'revenue-2Q': np.nan,
#                                                  'revenue-3Q': np.nan,
#                                                  'revenue-0Y': np.nan,
#                                                  'revenue-1Y': np.nan,
#                                                  'revenue-2Y': np.nan,
#                                                  'trailingPE': np.nan,
#                                                  'trailingEps': np.nan,
#                                                  'priceToSalesTrailing12Months': np.nan,
#                                                  'quickRatio': np.nan,
#                                                  'currentRatio': np.nan,
#                                                  'debtToEquity': np.nan,
#                                                  'profitMargins': np.nan,
#                                                  'pegRatio': np.nan,
#                                                  'twoHundredDayAverage': stock_info['twoHundredDayAverage'],
#                                                  'fiftyDayAverage': stock_info['fiftyDayAverage'],
#                                                  'fiftyTwoWeekHigh': stock_info['fiftyTwoWeekHigh'],
#                                                  'dividendRate': stock_info['dividendRate'],
#                                                  'industry': np.nan,
#                                                  'sector': np.nan}))
#
#                 except:
#                     pass
#
#             else:
#                 pass
#
#         # if exception occurs, print
#         except Exception as e:
#             print(e)
#             pass
#
#         # pass for other exception
#         except:
#             time.sleep(3)
#             pass
#
#     # convert dictionary list to dataframe
#     df = pd.DataFrame(list_financials, columns=['symbol',
#                                                 'earnings-0Q', 'earnings-1Q', 'earnings-2Q', 'earnings-3Q',
#                                                 'earnings-0Y', 'earnings-1Y', 'earnings-2Y',
#                                                 'revenue-0Q', 'revenue-1Q', 'revenue-2Q', 'revenue-3Q',
#                                                 'revenue-0Y', 'revenue-1Y', 'revenue-2Y',
#                                                 'trailingPE', 'trailingEps', 'priceToSalesTrailing12Months',
#                                                 'quickRatio', 'currentRatio', 'debtToEquity',
#                                                 'profitMargins', 'pegRatio',
#                                                 'twoHundredDayAverage', 'fiftyDayAverage',
#                                                 'fiftyTwoWeekHigh', 'dividendRate', 'industry', 'sector'])
#
#     # using dictionary to convert specific columns
#     convert_dict = {
#         'earnings-0Q': float,
#         'earnings-1Q': float,
#         'earnings-2Q': float,
#         'earnings-3Q': float,
#         'earnings-0Y': float,
#         'earnings-1Y': float,
#         'earnings-2Y': float,
#         'revenue-0Q': float,
#         'revenue-1Q': float,
#         'revenue-2Q': float,
#         'revenue-3Q': float,
#         'revenue-0Y': float,
#         'revenue-1Y': float,
#         'revenue-2Y': float,
#         'trailingPE': float,
#         'trailingEps': float,
#         'priceToSalesTrailing12Months': float,
#         'quickRatio': float,
#         'currentRatio': float,
#         'debtToEquity': float,
#         'profitMargins': float,
#         'pegRatio': float,
#         'twoHundredDayAverage': float,
#         'fiftyDayAverage': float,
#         'fiftyTwoWeekHigh': float,
#         'dividendRate': float,
#         'industry': str,
#         'sector': str,
#     }
#
#     # convert dataframe data types, replace inf to nan
#     df_financials = df.astype(convert_dict)
#     df_financials = df_financials.replace([np.inf, -np.inf], np.nan)
#
#     # send to SQL
#     df_financials.to_sql(name='stock_financials', con=connection_2, if_exists='replace')
#
#     return "Financials Created and Sent to SQL"
