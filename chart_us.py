import pandas as pd
from decimal import Decimal
from dataclasses import dataclass
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates

from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,
                               AutoMinorLocator)
import sys
import getopt
import requests
import time
import datetime
import mysql.connector

g_mydb = ''

@dataclass
class cases:
    country_name: str
    state_name:   str 
    update_dt:    datetime
    confirmed:    int
    deaths:       int
    recovered:    int
    lat:          Decimal
    long_:        Decimal 

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
    #fig, ax = plt.subplots(n,n)

    # Legend
    red_patch = mpatches.Patch(color='r', label = "Deaths")
    blue_patch = mpatches.Patch(color='b', label = "Confirmed Cases")
    #green_patch = mpatches.Patch(color='g', label = "Recovered Cases")
    #plt.legend(handles=[blue_patch,red_patch,green_patch],loc='upper left')
    #plt.legend (handles=[blue_patch,red_patch,green_patch], loc='upper left')
    #plt.figlegend (handles=[blue_patch,red_patch,green_patch], loc='upper left')
    plt.figlegend (handles=[blue_patch,red_patch], loc='upper left')
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
                
                confirmed = confirmed.append ({'Update Date': row[0], 'Volume': row[1]}, ignore_index=True)
                deaths    = deaths.append ({'Update Date': row[0], 'Volume': row[2]}, ignore_index=True)
                recovered = recovered.append ({'Update Date': row[0], 'Volume': row[3]}, ignore_index=True)
                date_max = row[0]

            #print (confirmed)
            ax[i][j].set_title(titles[i][j])
            ax[i][j].plot('Update Date', 'Volume', data=confirmed, color='b')
            ax[i][j].plot('Update Date', 'Volume', data=deaths,    color='r')
            #ax[i].plot('Update Date', 'Volume', data=recovered, color='g')

            # format the ticks
            ax[i][j].xaxis.set_major_locator(mdates.AutoDateLocator())
            ax[i][j].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax[i][j].xaxis.set_minor_locator(mdates.DayLocator())
            #ax[i].xaxis.set_minor_locator(AutoMinorLocator())

            # round to nearest years.
            #datemin = np.datetime64(data['date'][0], 'Y')
            #datemax = np.datetime64(data['date'][-1], 'Y') + np.timedelta64(1, 'Y')
            ax[i][j].set_xlim(date_min, date_max+datetime.timedelta(days=1))
            #ax[i][j].set_xlim(date_min, date_max)

            # format the coords message box
            ax[i][j].format_xdata = mdates.DateFormatter('%Y-%m-%d')
            ax[i][j].format_ydata = lambda x: '%d' % x  # format the price.
            ax[i][j].grid(True)

            # rotates and right aligns the x labels, and moves the bottom of the
            # axes up to make room for them
            fig.autofmt_xdate()
            fig.set_size_inches (14, 8, True)  
            #plt.title (titles[i])

            # Set Options
            plt.grid(True)
            #plt.yscale('log')

            j = j + 1

        i = i + 1

def main():
    global g_mydb


    g_mydb = mySql_DB()    
    g_mydb.connect()

    rows_it = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 2 and d.update_dt > '2020-03-01' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_us = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 3 and d.update_dt > '2020-03-01' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_il = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 22 and d.update_dt > '2020-03-01' group by date(d.update_dt) order by date(d.update_dt) asc")
    rows_ck = g_mydb.query_fetch_all("SELECT date(d.update_dt) as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM f_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_county_id = 265 and d.update_dt > '2020-03-01' group by date(d.update_dt) order by date(d.update_dt) asc")

    rows_it2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 2 and d.update_dt > '2020-03-01' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_us2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_country_id = 3 and d.update_dt > '2020-03-01' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_il2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_state_id = 22 and d.update_dt > '2020-03-01' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_ck2 = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_county_id = 265 and d.update_dt > '2020-03-01' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")
    rows_ny = g_mydb.query_fetch_all("SELECT date(a.update_dt) as update_dt, max(confirmed_cases) as confirmed_cases, max(deaths) as deaths, max(recovered_cases) as recovered_cases FROM (SELECT d.update_dt as update_dt, sum(f.confirmed_cases) as confirmed_cases, sum(f.deaths) as deaths, sum(f.recovered_cases) as recovered_cases FROM ts_rate3 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where f.d_county_id in (28, 52, 3289) and d.update_dt > '2020-03-01' group by d.update_dt order by d.update_dt asc) a group by date(a.update_dt) order by 1;")

    g_mydb.disconnect()

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
    titles [0][0] = 'Italy Cases'
    titles [0][1] = 'US Cases'
    titles [0][2] = 'Illinois Cases'
    titles [1][0] = 'Italy-ts Cases'
    titles [1][1] = 'US-ts Cases'
    titles [1][2] = 'Illinois-ts Cases'
    titles [2][0] = 'Cook County Cases'
    titles [2][1] = 'Cook County-ts Cases'
    titles [2][2] = 'New York County-ts Cases'

    plot_data (data, titles, n, n, 'US Coronavirus Dashboard')
    
    plt.show()

if __name__ == '__main__':
    main()