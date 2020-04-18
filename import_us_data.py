import sys
import getopt
import requests
import time
import datetime
import mysql.connector
import csv

g_mydb = ""

class mySql_DB:
    mysql_db = ""

    def connect (self, db_host="localhost", db_user="virus", db_password="corona", db_database="coronavirus", db_auth="mysql_native_password"):
        # Connect to mySQL database
        self.mysql_db = mysql.connector.connect (host=db_host, user=db_user, passwd=db_password, database=db_database, auth_plugin=db_auth)

    def disconnect (self):
        self.mysql_db.commit()
        self.mysql_db.close()

    def commit (self):
        self.mysql_db.commit()

    def execute_insert (self, insert_ddl):
        cursor = self.mysql_db.cursor()
        #print (insert_ddl)
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

def validate_data (value, dv = 0):
    if not value:
        value = dv
    return value

def get_country_id (country, lat, long_):
    country_id = -1

    # De-reference single quotes
    country_tmp = country.replace ("'", "\\'")
    sql_query_d_country  = ("SELECT d_country_id FROM d_country WHERE name='%s'") % (country_tmp)
    sql_insert_d_country = ("INSERT INTO d_country (name, lat, long_) VALUES ('%s', %f, %f)") % (country_tmp, lat, long_)

    return_row = g_mydb.query_fetch_one (sql_query_d_country)

    if not return_row is None:
        country_id = return_row [0]
    else:
        country_id = g_mydb.execute_insert (sql_insert_d_country)
        #print ("Country ID not found for: " + country)

    return country_id

def get_state_id (state, lat, long_, country_id):
    state_id = -1

    # De-reference single quotes
    state_tmp = state.replace ("'", "\\'")
    sql_query_d_state  = ("SELECT d_state_id FROM d_state WHERE name='%s' and d_country_id=%i") % (state_tmp, country_id)
    sql_insert_d_state = ("INSERT INTO d_state (name, lat, long_, d_country_id) VALUES ('%s', %f, %f, %i)") % (state, lat, long_, country_id)
    
    return_row = g_mydb.query_fetch_one (sql_query_d_state)

    if not return_row is None:
        state_id = return_row [0]
    else:
        state_id = g_mydb.execute_insert (sql_insert_d_state)

    return state_id

def get_county_id (county, lat, long_, country_id, state_id):
    global g_mydb
    county_id = -1

    # De-reference single quotes
    county_tmp = county.replace ("'", "\\'")
    sql_query_d_county  = ("SELECT d_county_id FROM d_county WHERE name='%s' and d_country_id=%i and d_state_id=%i") % (county_tmp, country_id, state_id)
    sql_insert_d_county = ("INSERT INTO d_county (name, lat, long_, d_country_id, d_state_id) VALUES ('%s', %f, %f, %i, %i)") % (county_tmp, lat, long_, country_id, state_id)
    
    return_row = g_mydb.query_fetch_one (sql_query_d_county)

    if not return_row is None:
        county_id = return_row [0]
    else:
        county_id = g_mydb.execute_insert (sql_insert_d_county)

    return county_id

def get_datetime_id (update_dt):
    global g_mydb
    datetime_id = -1

    sql_query_d_datetime  = ("SELECT d_datetime_id FROM d_datetime WHERE update_dt='%s'") % (update_dt)
    sql_insert_d_datetime = ("INSERT INTO d_datetime (update_dt) VALUES ('%s')") % (update_dt)
    
    return_row = g_mydb.query_fetch_one (sql_query_d_datetime)

    if not return_row is None:
        datetime_id = return_row [0]
    else:
        datetime_id = g_mydb.execute_insert (sql_insert_d_datetime)

    #print (datetime_id)
    return datetime_id

def insert_ts_rate4 (data_rec):
    ts_rate_id = -1

    country_id = data_rec["country_id"]
    state_id   = data_rec["state_id"]
    county_id  = data_rec["county_id"]
    date_id    = data_rec["date_id"]
    confirmed  = int(validate_data(data_rec["confirmed_cases"]))
    deaths     = int(validate_data(data_rec["deaths"]))

    # Query if the record is already there
    if not county_id is None:
        sql_query_ts_rate4 = ("SELECT ts_rate4_id FROM ts_rate4 WHERE d_country_id = %i and d_state_id = %i and d_county_id = %i and d_datetime_id = %i ORDER BY ts_rate4_id DESC LIMIT 1") % (country_id, state_id, county_id, date_id)
    elif not state_id is None:
        sql_query_ts_rate4 = ("SELECT ts_rate4_id FROM ts_rate4 WHERE d_country_id = %i and d_state_id = %i and d_county_id is null and d_datetime_id = %i ORDER BY ts_rate4_id DESC LIMIT 1") % (country_id, state_id, date_id)
    else:
        sql_query_ts_rate4 = ("SELECT ts_rate4_id FROM ts_rate4 WHERE d_country_id = %i and d_state_id is null and d_county_id is null and d_datetime_id = %i ORDER BY ts_rate4_id DESC LIMIT 1") % (country_id, date_id)
    ts_row = g_mydb.query_fetch_one (sql_query_ts_rate4)

    if ts_row is None:
        if not county_id is None:
            sql_insert_ts_rate4 = ("INSERT INTO ts_rate4 (confirmed_cases, deaths, d_datetime_id, d_country_id, d_state_id, d_county_id) VALUES (%i, %i, %i, %i, %i, %i)") % (confirmed, deaths, date_id, country_id, state_id, county_id)
        elif not state_id is None:
            sql_insert_ts_rate4 = ("INSERT INTO ts_rate4 (confirmed_cases, deaths, d_datetime_id, d_country_id, d_state_id) VALUES (%i, %i, %i, %i, %i)") % (confirmed, deaths, date_id, country_id, state_id)
        else:
            sql_insert_ts_rate4 = ("INSERT INTO ts_rate4 (confirmed_cases, deaths, d_datetime_id, d_country_id) VALUES (%i, %i, %i, %i)") % (confirmed, deaths, date_id, country_id)
        ts_rate_id = g_mydb.execute_insert (sql_insert_ts_rate4)

    return ts_rate_id

def insert_f_rate4 (data_rec):
    f_rate_id  = -1
    country_id = data_rec["country_id"]
    state_id   = data_rec["state_id"]
    county_id  = data_rec["county_id"]
    date_id    = data_rec["date_id"]
    confirmed  = int(validate_data(data_rec["confirmed_cases"]))
    deaths     = int(validate_data(data_rec["deaths"]))

    if confirmed !=0 or deaths != 0:
        # Query if the record is already there
        if not county_id is None:
            sql_query_f_rate4 = ("SELECT f_rate4_id FROM f_rate4 WHERE d_country_id = %i and d_state_id = %i and d_county_id = %i and d_datetime_id = %i ORDER BY f_rate4_id DESC LIMIT 1") % (country_id, state_id, county_id, date_id)
        elif not state_id is None:
            sql_query_f_rate4 = ("SELECT f_rate4_id FROM f_rate4 WHERE d_country_id = %i and d_state_id = %i and d_county_id is null and d_datetime_id = %i ORDER BY f_rate4_id DESC LIMIT 1") % (country_id, state_id, date_id)
        else:
            sql_query_f_rate4 = ("SELECT f_rate4_id FROM f_rate4 WHERE d_country_id = %i and d_state_id is null and d_county_id is null and d_datetime_id = %i ORDER BY f_rate4_id DESC LIMIT 1") % (country_id, date_id)
        f_row = g_mydb.query_fetch_one (sql_query_f_rate4)

        if f_row is None:
            # Write a new record if there is a change (non-zero)
            if not county_id is None:
                sql_insert_f_rate4 = ("INSERT INTO f_rate4 (confirmed_cases, deaths, d_datetime_id, d_country_id, d_state_id, d_county_id) VALUES (%i, %i, %i, %i, %i, %i)") % (confirmed, deaths, date_id, country_id, state_id, county_id)
            elif not state_id is None:
                sql_insert_f_rate4 = ("INSERT INTO f_rate4 (confirmed_cases, deaths, d_datetime_id, d_country_id, d_state_id) VALUES (%i, %i, %i, %i, %i)") % (confirmed, deaths, date_id, country_id, state_id)
            else:
                sql_insert_f_rate4 = ("INSERT INTO f_rate4 (confirmed_cases, deaths, d_datetime_id, d_country_id) VALUES (%i, %i, %i, %i)") % (confirmed, deaths, date_id, country_id)
            f_rate_id = g_mydb.execute_insert (sql_insert_f_rate4)

    return f_rate_id

def load_data (file_name, data_list, combined, combined_fact, col_name, col_start):
    first_row = True
    row_header = []
    date_id = {}
    # Open the CSV file
    with open ("../COVID-19/csse_covid_19_data/csse_covid_19_time_series/" + file_name, newline='') as f:
        reader = csv.reader (f)
        for row in reader:
            if first_row:
                first_row = False
                row_header = row
                # Update the data columns to be consistent YYYY-MM-DD HH:MM:SS
                for i in range (col_start , len (row_header), 1):
                    column_name = str (row_header [i])
                    from datetime import datetime 
                    new_column_name = str(datetime.strptime(column_name,'%m/%d/%y'))
                    new_date_id = get_datetime_id (new_column_name)
                    date_id.update([(column_name, new_date_id)])
                    #print (date_id)
            else:
                county  = str(validate_data(row [5], ''))
                state   = str(validate_data(row [6], ''))
                country = str(validate_data(row [7], ''))
                lat     = float(validate_data(row [8]))
                long_   = float(validate_data(row [9]))

                # Get the Country ID                
                country_id = get_country_id (country, lat, long_)

                if state == '' or state.isspace():
                    state_id = None
                else:
                    state_id   = get_state_id (state, lat, long_, country_id)

                if county == 'unassigned' or county == "Unassigned": 
                    county = ''

                county_id = None
                if state_id is None or county == '' or county.isspace():
                    county_id = None
                else:
                    county_id  = get_county_id  (county, lat, long_, country_id, state_id)

                if country_id == -1 or state_id == -1:
                    print (file_name, state, country, country_id, state_id, lat, long_)

                # Process the data columns
                previous_day_val = 0
                for i in range (col_start , len (row_header), 1):
                    cur_day_val      = int(validate_data(row[i]))
                    fact_val         = cur_day_val - previous_day_val
                    previous_day_val = cur_day_val
                    
                    # Check if key is in Combined
                    combined_key = str(country_id)+"-"+str(state_id)+"-"+str(county_id)+"-"+str(date_id [row_header[i]])
                    
                    if combined_key in combined:
                        new_data = combined[combined_key]
                        new_data [col_name] = cur_day_val
                    else:
                        # Add the key
                        new_data = {'country_id'      : country_id,
                                    'state_id'        : state_id,
                                    'county_id'       : county_id,
                                    'date_id'         : date_id [row_header[i]],
                                    'update_dt'       : row_header [i],
                                    'confirmed_cases' : 0,
                                    'deaths'          : 0,
                                    'recovered_cases' : 0
                                   }
                        new_data [col_name] = cur_day_val

                    combined.update([(combined_key, new_data)])        

                    if combined_key in combined_fact:
                        new_data_fact = combined_fact[combined_key]
                        new_data_fact [col_name] = fact_val
                    else:
                        # Add the key
                        new_data_fact = {'country_id'      : country_id,
                                         'state_id'        : state_id,
                                         'county_id'       : county_id,
                                         'date_id'         : date_id [row_header[i]],
                                         'update_dt'       : row_header [i],
                                         'confirmed_cases' : 0,
                                         'deaths'          : 0,
                                         'recovered_cases' : 0
                                        }
                        new_data_fact [col_name] = fact_val

                    combined_fact.update([(combined_key, new_data_fact)])        

                    #if country_id ==5 :
                    #    print (country, state, country_id, state_id, new_data)
                
            #print (row)

def insert_data (combined, combined_fact):
    global g_mydb
    records = 1
    commit_limit = 10000

    #print ("Combined rows to be inserted:", len(combined))
    #print ("Fact     rows to be inserted:", len(combined_fact))
    for key, value in combined.items():
        #print ("Combined", key, value)
        ts_id = insert_ts_rate4 (value)
        
        records = records + 1
        if records > commit_limit:
            g_mydb.commit()
            records = 1
        # print (ts_id)
    
    # Commit all records for ts_rate4
    g_mydb.commit()

    from datetime import datetime
    print (str(datetime.now()), " Finished ts_rate4")

    records = 1
    for key, value in combined_fact.items():
        f_id = insert_f_rate4 (value)

        records = records + 1
        if records > commit_limit:
            g_mydb.commit()
            records = 1
        #print (f_id)
        #if value ['country_id'] == 5:
        #if value["confirmed_cases"] != 0 and value["deaths"] != 0:
        #    print ("Fact", key, value)

    # Commit all records for f_rate4
    g_mydb.commit()

    from datetime import datetime
    print (str(datetime.now()), " Finished f_rate4")

def main ():
    global g_mydb

    combined      = {}
    combined_fact = {}
    confirmed     = []
    deaths        = []

    # Connect to mySQL database
    g_mydb = mySql_DB()    
    g_mydb.connect()

    from datetime import datetime
    print (str(datetime.now()))

    load_data ("time_series_covid19_confirmed_US.csv", confirmed, combined, combined_fact, 'confirmed_cases', 11)
    load_data ("time_series_covid19_deaths_US.csv", deaths, combined, combined_fact, 'deaths', 12)

    insert_data (combined, combined_fact)
    
    # Close the connection
    g_mydb.disconnect()


if __name__ == '__main__':
    main()
