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

def main():
    global g_mydb

    confirmed = pd.DataFrame({'Update Date': [], 'Volume': []}) 
    deaths    = pd.DataFrame({'Update Date': [], 'Volume': []}) 
    recovered = pd.DataFrame({'Update Date': [], 'Volume': []}) 

    g_mydb = mySql_DB()
    
    g_mydb.connect()

    rows = g_mydb.query_fetch_all("SELECT c.name, d.update_dt, f.confirmed_cases, f.deaths, f.recovered_cases FROM ts_rate2 f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id inner join d_country c on f.d_country_id = c.d_country_id where f.d_country_id = 5 order by d.update_dt asc")

    first = True
    date_min = ''
    date_max = ''
    for row in rows:
        # print (row)
        if first:
            first = False
            date_min = row[1]
        confirmed = confirmed.append ({'Update Date': row[1], 'Volume': row[2]}, ignore_index=True)
        deaths    = deaths.append ({'Update Date': row[1], 'Volume': row[3]}, ignore_index=True)
        recovered = recovered.append ({'Update Date': row[1], 'Volume': row[4]}, ignore_index=True)
        date_max = row[1]

    #print (confirmed)
    g_mydb.disconnect()
    
    fig, ax = plt.subplots()
    plt.yscale('log')
    plt.grid(True)

    ax.plot('Update Date', 'Volume', data=confirmed, color='b')
    ax.plot('Update Date', 'Volume', data=deaths,    color='r')
    ax.plot('Update Date', 'Volume', data=recovered, color='g')

    # format the ticks
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    #ax.xaxis.set_minor_locator(mdates.DayLocator())
    ax.xaxis.set_minor_locator(AutoMinorLocator())

    # round to nearest years.
    #datemin = np.datetime64(data['date'][0], 'Y')
    #datemax = np.datetime64(data['date'][-1], 'Y') + np.timedelta64(1, 'Y')
    ax.set_xlim(date_min, date_max)

    # format the coords message box
    ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
    ax.format_ydata = lambda x: '%d' % x  # format the price.
    ax.grid(True)

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    fig.autofmt_xdate()

    plt.show()
'''
    plt.figure('Coronavirus US Cases', [12,12])
    plt.plot(confirmed ['Update Date'], confirmed ['Volume'], color='b') 
    plt.plot(deaths    ['Update Date'], deaths    ['Volume'], color='r') 
    plt.plot(recovered ['Update Date'], recovered ['Volume'], color='g') 
    plt.xlabel('Date', fontsize=10)
    plt.ylabel('Cases', fontsize=10)

    red_patch = mpatches.Patch(color='r', label = "Deaths")
    blue_patch = mpatches.Patch(color='b', label = "Confirmed Cases")
    green_patch = mpatches.Patch(color='g', label = "Recovered Cases")
    plt.legend(handles=[blue_patch,red_patch,green_patch])
    #plt.axvline(underlying.last_sale, color='g')
    plt.title("Coronavirus US Cases", fontsize=12)

    plt.show()
 '''
if __name__ == '__main__':
    main()