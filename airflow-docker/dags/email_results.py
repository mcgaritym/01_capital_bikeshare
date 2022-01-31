# import libraries
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pretty_html_table import build_table
import pandas as pd
from sqlalchemy import create_engine
from config import *
from AWSConnect import AWSConnect

# def connect_RDS():
#
#     # specify second MySQL database connection (faster read_sql query feature)
#     connection = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=rds_user,
#                                                                     password=rds_pwd, host=rds_host,
#                                                                     port=rds_port, db=rds_database))
#     return connection
#
# def close_RDS():
#
#     try:
#         connect_RDS().dispose()
#
#     except:
#         pass

# connect to SQL and create database, table
def email_results(sender, receiver, email_subject):

    # get class, and create connections
    connect = AWSConnect(rds_user, rds_pwd, rds_host, rds_port, rds_database, service_name, region_name, aws_access_key_id, aws_secret_access_key)
    rds_sqlalchemy = connect.rds_sqlalchemy()

    df = pd.read_sql_query("SELECT * FROM recent_rides;", con=rds_sqlalchemy)

    # specify credentials
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = sender
    receiver_email = [receiver]
    password = GMAIL_PASSWORD

    # build HTML body with dataframe
    email_html = """
    <html>
      <body>
        <p>Hello, here are the most recent rides: </p> <br>
        {0}
      </body>
    </html>
    """.format(build_table(df, 'blue_light', font_size='large'))

    message = MIMEMultipart("multipart")
    # Turn these into plain/html MIMEText objects
    part2 = MIMEText(email_html, "html")
    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part2)
    message["Subject"] = email_subject
    message["From"] = sender_email

    ## iterating through the receiver list
    for i, val in enumerate(receiver):
        message["To"] = val
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())

    return print("Recent Rides Email Successful")
