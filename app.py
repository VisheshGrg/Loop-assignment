from flask import Flask, render_template, redirect, url_for, request, session
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
import sqlite3
import mysql.connector
import pytz
import random
import datetime
import string

app = Flask(__name__)

# This function will generate the random strings of the given length which will use to provide the file name
def generate_random_string(length):
    letters=string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


@app.route('/trigger_report')
def trigger_report():

    #read all csv files
    df_store_status=pd.read_csv('store_status.csv');
    df_timezone=pd.read_csv('time_zone.csv')
    df_menu_hours=pd.read_csv('menu_hours.csv')
    nm=generate_random_string(1)
    conn=sqlite3.connect(f'user7.db')

    # connecting to all sql server
    df_store_status.to_sql('df_store_status', conn, if_exists='replace', index=False)
    df_timezone.to_sql('df_timezone', conn, if_exists='replace', index=False)
    df_menu_hours.to_sql('df_menu_hours', conn, if_exists='replace', index=False)


    time_diff=[]
    for i in range(len(df_timezone)):
        #Get the current time in the specified time zone
        dt_dynamic=datetime.datetime.now(pytz.timezone(df_timezone['timezone_str'][i]))
        # Extract the hour offset from the time zone and converted it into integer
        time_diff.append(int(dt_dynamic.strftime('%z')[:3]))
    df_timezone['time_diff']=time_diff
    print('The df_timezone is ', df_timezone['time_diff'].head())

    # The query takes the timestamp from the df_store_status and gives the default timezone to 'America/Chicago as mentioned in the question and on the basis of the date it finds the day of the week'
    query='''
        SELECT df_store_status.store_id, df_store_status.status, df_store_status.timestamp_utc, COALESCE(df_timezone.timezone_str, 'America/Chicago') AS timezone_str,
        substr(df_store_status.timestamp_utc,1,10) as Date,
        CASE (strftime('%w', substr(df_store_status.timestamp_utc,1,10))-1)%7
            When 0 THEN 'Monday'
            When 1 THEN 'Tuesday'
            When 2 THEN 'Wednesday'
            When 3 THEN 'Thursday'
            When 4 THEN 'Friday'
            When 5 THEN 'Saturday'
            When -1 THEN 'Sunday'
            ELSE 'Unknown'
        END AS Day
        FROM df_store_status
        LEFT JOIN df_timezone
        ON df_store_status.store_id = df_timezone.store_id
    '''

    # Stores the result of the above query in df_id data frame
    df_id = pd.read_sql_query(query,conn)
    print('The df_id is: ',df_id.head())


    timestamp_timezone=[]
    rows_to_drop = []
    i=1
    # Loop through each item of the df_id data frame
    for i in range(len(df_id)):
        try:
            #Parse the timestamp_utc string into a datetime object
            given_datetime = datetime.datetime.strptime(df_id['timestamp_utc'][i], "%Y-%m-%d %H:%M:%S.%f %Z")
            #Get the current time in specified timezone
            dt_dynamic=datetime.datetime.now(pytz.timezone(df_id['timezone_str'][i]))
            #Calculates the time difference
            time_difference=int(dt_dynamic.strftime('%z')[:3])
            time_difference=datetime.timedelta(hours=time_difference)
            #Adjusts the given datetime by adding the time difference
            result_datetime = given_datetime + time_difference
            #Convert it back to the timestamp string
            result_timestamp = result_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            timestamp_timezone.append(result_timestamp)
        except:
            #If exception occurs then store the index of the item in rows_to_drop
            rows_to_drop.append(i)
            print(i)
            i+=1

    #Drop the exception rows from the data frame and make a new column in data frame for timestamp_timezone
    df_id = df_id.drop(rows_to_drop)
    df_id['timestamp_timezone']=timestamp_timezone

    #make a map of all the days in the week
    day_map={
        'Monday':0,
        'Tuesday':1,
        'Wednesday':2,
        'Thursday':3,
        'Friday':4,
        'Saturday':5,
        'Sunday':6
    }

    #swapping the key-value
    reverse_day_map = {v: k for k, v in day_map.items()}
    print(reverse_day_map)

    #making a new column in menu_hours data frame for the day of the week as per the day_map
    df_menu_hours=pd.read_csv('menu_hours.csv')
    df_menu_hours['DayOfWeek']=df_menu_hours['day'].map(reverse_day_map)

    #connecting the menu_hours and df_id data frame to sql
    df_menu_hours.to_sql('df_menu_hours', conn, if_exists='replace', index=False)
    df_id.to_sql('df_id', conn, if_exists='replace', index=False)

    # This query finds all stores which are active between the start_time and end_time by combining menu_hours and df_id data frames
    query = ''' 
        SELECT df_id.store_id, df_id.day, df_id.status, df_id.timestamp_utc, df_id.timezone_str, df_id.Date, df_id.timestamp_timezone,
        df_menu_hours.start_time_local, df_menu_hours.end_time_local, df_menu_hours.DayOfWeek
        FROM df_id
        LEFT JOIN df_menu_hours
        ON df_id.store_id = df_menu_hours.store_id
        WHERE df_menu_hours.DayOfWeek = df_id.day
        AND df_id.status='active'
        AND time(df_id.timestamp_timezone) >= time(df_menu_hours.start_time_local)
        AND time(df_id.timestamp_timezone) <= time(df_menu_hours.end_time_local)
    ''' 

    #Storing the result to df_filter data frame
    df_filter = pd.read_sql_query(query, conn)
    print("the df_filter is ",df_filter.head())

    #making a new column in df_filter frame to store timestamp_timezone calculated in prev query and storing minutes by extracting it from the timestamp_timezone
    df_filter['timestamp_timezone']=pd.to_datetime(df_filter['timestamp_timezone'])
    df_filter['minute']=df_filter['timestamp_timezone'].dt.minute

    #change the minute values by subracting it from 60 which gives the resulting minutes
    df_filter['minute']=df_filter['minute'].apply(lambda x: 60-x)

    #renaming minutes column to uptime_last_hour
    df_filter.rename(columns={'minute': 'uptime_last_hour'}, inplace=True)
    #making a new column for downtime_last_hour having values same as the minutes column
    df_filter['downtime_last_hour']=60-df_filter['uptime_last_hour']
    #Drop day column
    df_filter=df_filter.drop(['Day'],axis=1)
    df_filter.to_sql('df_filter', conn, if_exists='replace', index=False)

    #This query gives sum of uptime for each day, formatted start and end times, and the total end times, and the total time difference between start and end times
    query = ''' 
        SELECT df_filter.store_id, df_filter.DayOfWeek,df_filter.Date,df_filter.timestamp_timezone , 
        SUM(df_filter.uptime_last_hour) AS uptime_last_day,
        strftime('%H:%M:%S', df_filter.start_time_local) AS start_time_local,
        strftime('%H:%M:%S', df_filter.end_time_local) AS end_time_local,
        (julianday(df_filter.end_time_local) - julianday(df_filter.start_time_local)) * 1440 AS total_time
        FROM df_filter
        GROUP BY df_filter.store_id, df_filter.DayOfWeek, df_filter.Date
    ''' 


    df_filter2 = pd.read_sql_query(query, conn)
    #Making a new column on the basis of total time and uptime last day in the df_filter2 data frame
    df_filter2['downtime_last_day']=df_filter2['total_time']-df_filter2['uptime_last_day']
    print("The df_filter2 is ",df_filter2.head())
    #Connecting to sql
    df_filter2.to_sql('df_filter2', conn, if_exists='replace', index=False)


    #This query will give dataset and aggregated information for each store_id
    query='''
        SELECT store_id, SUM(uptime_last_day) AS uptime_last_week, 
        CAST(ROUND(SUM(downtime_last_day),0)AS INTEGER) AS downtime_last_week,
        CAST(ROUND(SUM(total_time),0) AS INTEGER) AS total_time_last_week
        FROM df_filter2
        GROUP BY store_id
    '''
    
    df_filter3 = pd.read_sql_query(query, conn)
    print("The df_filter3 is ",df_filter3.head())
    df_filter3.to_sql('df_filter3', conn, if_exists='replace', index=False)


    # This combines results of df_filte2 and db_filter3 data frame on the basis of store_id as a common field
    query='''
        SELECT df_filter2.store_id,df_filter2.DayOfWeek, df_filter2.timestamp_timezone , df_filter2.uptime_last_day, df_filter2.downtime_last_day, df_filter3.uptime_last_week, df_filter3.downtime_last_week
        FROM df_filter2
        LEFT JOIN df_filter3
        ON df_filter2.store_id = df_filter3.store_id 
    '''

    df_filter4 = pd.read_sql_query(query, conn)
    df_filter4.to_sql('df_filter4', conn, if_exists='replace', index=False)
    print("The df_filter4 is ",df_filter4.head())


    # Combines the results of the df_filter and df_filter4 data frame
    query='''
        SELECT df_filter.store_id,df_filter.DayOfWeek, df_filter.timestamp_timezone, df_filter.uptime_last_hour, df_filter.downtime_last_hour, ROUND((df_filter4.uptime_last_day)/60,2) AS uptime_last_day, ROUND((df_filter4.downtime_last_day)/60,2) AS downtime_last_day, ROUND((df_filter4.uptime_last_week)/60,2) AS uptime_last_week, ROUND((df_filter4.downtime_last_week)/60,2) AS downtime_last_week
        FROM df_filter
        LEFT JOIN df_filter4
        ON df_filter.store_id = df_filter4.store_id 
    '''


    df_filter5 = pd.read_sql_query(query, conn)
    df_filter5.head(7)
    print("The df_filter5 is",df_filter5.head(7))

    df_filter5['downtime_last_day']=abs(df_filter5['downtime_last_day'])
    df_filter5['downtime_last_week']=abs(df_filter5['downtime_last_week'])
    
    df_filter5.to_csv('df_filter5.csv', index=False)
    random_string = generate_random_string(10)
    print("The random string is ",random_string)
    #Name of the file is given as random string generated of length 10
    df_filter5.to_csv(f'{random_string}.csv', index=False)
    return f"The random string that is generated is {random_string}"
    
#get_report api for getting the file
@app.route('/get_report', methods=['GET'])
def get_report():
    textt = request.args.get('text')
    return "Your .csv file has been saved"

if __name__ == '__main__':
    app.run(debug=True)