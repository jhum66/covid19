import pandas as pd
from decimal import Decimal
from dataclasses import dataclass
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates

from matplotlib.ticker import (MultipleLocator, FormatStrFormatter, AutoMinorLocator, LogLocator, LogitLocator)
import sys
import getopt
import requests
import time
import datetime
import mysql.connector

g_mydb = ''

class mySql_DB:
    mysql_db = ""

    def connect (self, db_host="localhost", db_user="virus", db_password="corona", db_database="coronavirus", db_auth="mysql_native_password"):
        # Connect to mySQL database
        self.mysql_db = mysql.connector.connect (host=db_host, user=db_user, passwd=db_password, database=db_database, auth_plugin=db_auth)

    def disconnect (self):
        self.mysql_db.commit()
        self.mysql_db.close()

    def execute_insert (self, insert_ddl):
        cursor = self.mysql_db.cursor()
        # print (insert_ddl)
        cursor.execute (insert_ddl)

        last_id = cursor.lastrowid
        cursor.close()

        return last_id

    def execute_update (self, update_ddl):
        cursor = self.mysql_db.cursor()
        cursor.execute (update_ddl)
        cursor.close()

    def query_fetch_one (self, query_string):
        cursor = self.mysql_db.cursor()
        cursor.execute (query_string)
        cursor_row = cursor.fetchone()
        cursor.close()

        return cursor_row

    def query_fetch_all (self, query_string):
        cursor = self.mysql_db.cursor()
        cursor.execute (query_string)
        cursor_rows = cursor.fetchall()
        cursor.close()

        return cursor_rows

def plot_data (data, titles, chart_rows, chart_cols, figure_title):
    first = True
    date_min = ''
    date_max = ''

    fig = plt.figure(figsize=(14,8))
    fig.suptitle(figure_title, fontsize=14)
    ax  = fig.subplots(chart_rows, chart_cols)

    # Legend
    red_patch = mpatches.Patch(color='r', label = "Deaths")
    blue_patch = mpatches.Patch(color='b', label = "Confirmed Cases")
    green_patch = mpatches.Patch(color='g', label = "Recovered Cases")
    plt.figlegend (handles=[blue_patch,red_patch,green_patch], loc='upper left')
    plt.minorticks_on()

    i = 0
    while i < chart_rows:
        j = 0
        while j < chart_cols:
            confirmed = pd.DataFrame({'Update Date': [], 'Volume': []}) 
            deaths    = pd.DataFrame({'Update Date': [], 'Volume': []}) 
            recovered = pd.DataFrame({'Update Date': [], 'Volume': []}) 

            rows = data [i] [j]
            #print ("Lenght of rows:", len(rows))

            for row in rows:
                #print (row)
                if first:
                    first = False
                    date_min = row[0]

                confirmed_db = float (row[1])
                deaths_db    = float (row[2])
                recovered_db = float (row[3])                
                if confirmed_db < 0: 
                    #print ("C", confirmed_db)
                    confirmed_db = 0.1
                if deaths_db < 0: 
                    #print ("D", deaths_db)
                    deaths_db = 0.1
                if recovered_db < 0:
                    #print ("R", recovered_db) 
                    recovered_db = 0.1

                confirmed = confirmed.append ({'Update Date': row[0], 'Volume': confirmed_db}, ignore_index=True)
                deaths    = deaths.append ({'Update Date': row[0], 'Volume': deaths_db}, ignore_index=True)
                recovered = recovered.append ({'Update Date': row[0], 'Volume': recovered_db}, ignore_index=True)
                date_max = row[0]

            #print (confirmed)
            ax[i][j].set_title(titles[i][j])
            ax[i][j].plot('Update Date', 'Volume', data=confirmed, color='b')
            ax[i][j].plot('Update Date', 'Volume', data=deaths,    color='r')
            #ax[i].plot('Update Date', 'Volume', data=recovered, color='g')

            # format the ticks
            #print (LogLocator().MAXTICKS)
            #ax[i][j].yaxis.set_major_locator(LogLocator(10, 2, 2, 2000))
            ax[i][j].xaxis.set_major_locator(mdates.AutoDateLocator())
            ax[i][j].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax[i][j].xaxis.set_minor_locator(mdates.DayLocator())
            #ax[i].xaxis.set_minor_locator(AutoMinorLocator())

            # round to nearest day.
            ax[i][j].set_xlim(date_min, date_max+datetime.timedelta(days=1))

            # format the coords message box
            ax[i][j].format_xdata = mdates.DateFormatter('%Y-%m-%d')
            ax[i][j].format_ydata = lambda x: '%d' % x  # format the price.
            ax[i][j].grid(True)

            # rotates and right aligns the x labels, and moves the bottom of the
            # axes up to make room for them
            fig.autofmt_xdate()
            fig.set_size_inches (14, 8, True)  

            # Set Options
            plt.grid(True)
            #plt.yscale('log')

            j = j + 1

        i = i + 1

def chart_us():
    global g_mydb

    rows_it = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 2 and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_us = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 3 and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_il = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 22 and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_ck = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_county_id = 265 and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_ny = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_county_id in (28, 52, 3289) and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")

    rows_it2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 2 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_us2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 3 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_il2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 22 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_ck2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_county_id = 265 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    #rows_ny = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_county_id in (28, 52, 3289) and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")

    n = 3
    data = [[0] * n for i in range (n)]
    data [0][0] = rows_it
    data [0][1] = rows_us
    data [0][2] = rows_il
    data [1][0] = rows_it2
    data [1][1] = rows_us2
    data [1][2] = rows_il2
    data [2][0] = rows_ck
    data [2][1] = rows_ck2
    data [2][2] = rows_ny

    titles = [[0] * n for i in range (n)]
    titles [0][0] = 'Italy Cases Daily Growth'
    titles [0][1] = 'US Cases Daily Growth'
    titles [0][2] = 'Illinois Cases Daily Growth'
    titles [1][0] = 'Italy Cases Total'
    titles [1][1] = 'US Cases Total'
    titles [1][2] = 'Illinois Cases Total'
    titles [2][0] = 'Cook County Cases Daily Growth'
    titles [2][1] = 'Cook County Cases Total'
    titles [2][2] = 'New York County Cases Daily Growth'

    plot_data (data, titles, n, n, 'US Coronavirus Dashboard')

def chart_ca():
    global g_mydb

    rows_it = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 2 and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_ca = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 18 and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_on = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 44 and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_bc = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 86 and d.update_dt > '2020-03-15' group by date(d.update_dt) order by date(d.update_dt) asc")

    rows_it2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 2 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_ca2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 18 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_on2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 44 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_bc2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 86 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_to = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 27 and d.update_dt > '2020-03-15' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")

    n = 3
    data = [[0] * n for i in range (n)]
    data [0][0] = rows_it
    data [0][1] = rows_ca
    data [0][2] = rows_on
    data [1][0] = rows_it2
    data [1][1] = rows_ca2
    data [1][2] = rows_on2
    data [2][0] = rows_bc
    data [2][1] = rows_bc2
    data [2][2] = rows_to

    titles = [[0] * n for i in range (n)]
    titles [0][0] = 'Italy Cases Daily Growth'
    titles [0][1] = 'Canada Cases Daily Growth'
    titles [0][2] = 'Ontario Cases Daily Growth'
    titles [1][0] = 'Italy Cases Total'
    titles [1][1] = 'Canada Cases Total'
    titles [1][2] = 'Ontario Cases Total'
    titles [2][0] = 'British Columbia Cases Daily Growth'
    titles [2][1] = 'British Columbia Cases Total'
    titles [2][2] = 'Quebec Cases Total'

    plot_data (data, titles, n, n, 'Canadian Coronavirus Dashboard')

def main():
    global g_mydb

    g_mydb = mySql_DB()    
    g_mydb.connect()

    chart_us()
    chart_ca()

    g_mydb.disconnect()
    
    plt.show(block=True)

if __name__ == '__main__':
    main()