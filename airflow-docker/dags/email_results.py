# # sql libraries
# import mysql.connector as msql
# import mysql.connector
# import os
# from config import *
# from datetime import timedelta
# import smtplib, ssl
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from pretty_html_table import build_table
# import pandas as pd
# from sqlalchemy import create_engine
#
# # connect to SQL and create database, table
# def email_results(sender, receiver, email_subject):
#
#     # get undervalued stocks df via SQL
#     connection_2 = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=MYSQL_USER,
#                                                                     password=MYSQL_ROOT_PASSWORD, host=MYSQL_HOST,
#                                                                     port=MYSQL_PORT, db=MYSQL_DATABASE))
#
#     df = pd.read_sql_query("SELECT symbol, `Last Sale`, `Market Cap`, industry, sector FROM undervalued_stocks", con=connection_2)
#
#     # specify credentials
#     port = 465  # For SSL
#     smtp_server = "smtp.gmail.com"
#     sender_email = sender
#     receiver_email = [receiver]
#     password = GMAIL_PASSWORD
#
#     # build HTML body with dataframe
#     email_html = """
#     <html>
#       <body>
#         <p>Hello, here are today's stock picks: </p> <br>
#         {0}
#       </body>
#     </html>
#     """.format(build_table(df, 'blue_light', font_size='large'))
#
#     message = MIMEMultipart("multipart")
#     # Turn these into plain/html MIMEText objects
#     part2 = MIMEText(email_html, "html")
#     # Add HTML/plain-text parts to MIMEMultipart message
#     # The email client will try to render the last part first
#     message.attach(part2)
#     message["Subject"] = email_subject
#     message["From"] = sender_email
#
#     ## iterating through the receiver list
#     for i, val in enumerate(receiver):
#         message["To"] = val
#         context = ssl.create_default_context()
#         with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
#             server.login(sender_email, password)
#             server.sendmail(sender_email, receiver_email, message.as_string())
#
#     return "Stock Picks Email Successful"
