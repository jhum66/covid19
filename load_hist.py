import sys
import getopt
import requests
import time
import datetime
import mysql.connector
import os
import csv

g_mydb = ''

class mySql_DB:
    mysql_db = ""

    def connect (self, db_host="localhost", db_user="virus", db_password="corona", db_database="coronavirus", db_auth="mysql_native_password"):
        # Connect to mySQL database
        self.mysql_db = mysql.connector.connect (host=db_host, user=db_user, passwd=db_password, database=db_database, auth_plugin=db_auth)

    def commit (self):
        self.mysql_db.commit()

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

def validate_data (value, dv = 0):
    if not value:
        value = dv
    return value

def get_country_id (country, lat, long_):
    global g_mydb

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
    global g_mydb
    state_id = -1

    # De-reference single quotes
    state_tmp = state.replace ("'", "\\'")
    sql_query_d_state  = ("SELECT d_state_id FROM d_state WHERE name='%s' and d_country_id=%i") % (state_tmp, country_id)
    sql_insert_d_state = ("INSERT INTO d_state (name, lat, long_, d_country_id) VALUES ('%s', %f, %f, %i)") % (state_tmp, lat, long_, country_id)
    
    return_row = g_mydb.query_fetch_one (sql_query_d_state)

    if not return_row is None:
        state_id = return_row [0]
    else:
        state_id = g_mydb.execute_insert (sql_insert_d_state)
        #if country_id == 5:
        #    print (sql_insert_d_state)

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
        #print (sql_insert_d_county)

    return county_id

def get_datetime_id (update_dt):
    global g_mydb
    datetime_id = -1

    sql_query_d_datetime  = ("SELECT d_datetime_id FROM d_datetime WHERE update_dt = '%s' ORDER BY d_datetime_id DESC LIMIT 1") % (update_dt)
    sql_insert_d_datetime = ("INSERT INTO d_datetime (update_dt) VALUES ('%s')") % (update_dt)
    
    #print (query_d_datetime)
    #print (insert_d_datetime)
    return_row = g_mydb.query_fetch_one (sql_query_d_datetime)

    if not return_row is None:
        datetime_id = return_row [0]
    else:
        #print (insert_d_datetime)
        datetime_id = g_mydb.execute_insert (sql_insert_d_datetime)

    #print (datetime_id)
    return datetime_id

def translate_state (state_cd):
    state_nm = ""

    if state_cd == "AL":
        state_nm = "Alabama"
    elif state_cd == "AK":
        state_nm = "Alaska"
    elif state_cd == "AZ":
        state_nm = "Arizona"
    elif state_cd == "AR":
        state_nm = "Arkansas"
    elif state_cd == "CA":
        state_nm = "California"
    elif state_cd == "CO":
        state_nm = "Colorado"
    elif state_cd == "CT":
        state_nm = "Connecticut"
    elif state_cd == "DE":
        state_nm = "Delaware"
    elif state_cd == "FL":
        state_nm = "Florida"
    elif state_cd == "GA":
        state_nm = "Georgia"
    elif state_cd == "HI":
        state_nm = "Hawaii"
    elif state_cd == "ID":
        state_nm = "Idaho"
    elif state_cd == "IL":
        state_nm = "Illinois"
    elif state_cd == "IN":
        state_nm = "Indiana"
    elif state_cd == "IA":
        state_nm = "Iowa"
    elif state_cd == "KS":
        state_nm = "Kansas"
    elif state_cd == "KY":
        state_nm = "Kentucky"
    elif state_cd == "LA":
        state_nm = "Louisiana"
    elif state_cd == "ME":
        state_nm = "Maine"
    elif state_cd == "MD":
        state_nm = "Maryland"
    elif state_cd == "MA":
        state_nm = "Massachusetts"
    elif state_cd == "MI":
        state_nm = "Michigan"
    elif state_cd == "MN":
        state_nm = "Minnesota"
    elif state_cd == "MS":
        state_nm = "Mississippi"
    elif state_cd == "MO":
        state_nm = "Missouri"
    elif state_cd == "MT":
        state_nm = "Montana"
    elif state_cd == "NE":
        state_nm = "Nebraska"
    elif state_cd == "NV":
        state_nm = "Nevada"
    elif state_cd == "NH":
        state_nm = "New Hampshire"
    elif state_cd == "NJ":
        state_nm = "New Jersey"
    elif state_cd == "NM":
        state_nm = "New Mexico"
    elif state_cd == "NY":
        state_nm = "New York"
    elif state_cd == "NC":
        state_nm = "North Carolina"
    elif state_cd == "ND":
        state_nm = "North Dakota"
    elif state_cd == "OH":
        state_nm = "Ohio"
    elif state_cd == "OK":
        state_nm = "Oklahoma"
    elif state_cd == "OR":
        state_nm = "Oregon"
    elif state_cd == "PA":
        state_nm = "Pennsylvania"
    elif state_cd == "RI":
        state_nm = "Rhode Island"
    elif state_cd == "SC":
        state_nm = "South Carolina"
    elif state_cd == "SD":
        state_nm = "South Dakota"
    elif state_cd == "TN":
        state_nm = "Tennessee"
    elif state_cd == "TX":
        state_nm = "Texas"
    elif state_cd == "UT":
        state_nm = "Utah"
    elif state_cd == "VT":
        state_nm = "Vermont"
    elif state_cd == "VA":
        state_nm = "Virginia"
    elif state_cd == "WA":
        state_nm = "Washington"
    elif state_cd == "WV":
        state_nm = "West Virginia"
    elif state_cd == "WI":
        state_nm = "Wisconsin"
    elif state_cd == "WY":
        state_nm = "Wyoming"
    elif state_cd == "DC" or state_cd == "D.C.":
        state_nm = "District of Columbia"
    elif state_cd == "MH":
        state_nm = "Marshall Islands"
    elif state_cd == "Chicago":
        state_nm = "Illinois"
    else:
        state_nm = state_cd
    
    return state_nm

def load_data (file_name, file_date):
    global g_mydb

    first_row = True
    row_header = []

    # Get the date once per file
    date_id = -1
    from datetime import datetime 
    last_update     = datetime.strptime(file_date, '%m-%d-%Y')
    last_update_str = str(last_update)
    date_id = get_datetime_id (last_update_str)

    # Open the CSV file
    with open (file_name, newline='') as f:
        reader = csv.reader (f)
        for row in reader:
            if first_row:
                first_row = False
                row_header = row
                if len(row_header) < 6:
                    print ("Bad file:", file_name)
                    return
            else:
                county = ''
                if len (row_header) == 6:
                    state           = str(validate_data(row [0], ''))
                    country         = str(validate_data(row [1], ''))

                    lat             = 0
                    long_           = 0
                    confirmed       = int(validate_data(row [3]))
                    deaths          = int(validate_data(row [4]))
                    recovered       = int(validate_data(row [5]))
                    active          = 0

                    # If there is a "," in the state, then it probably contains a county too
                    sep = state.find (",")
                    if sep > 0:
                        county = state[0:sep]
                        state = state [sep+2:]
                        #print (county, state)
                        state = translate_state (state)

                    sep = state.find ("(From Diamond Princess)")
                    if sep > 0:
                        state = state [0:sep].strip()
                        state = translate_state (state)

                elif len (row_header) == 8:
                    state           = str(validate_data(row [0], ''))
                    country         = str(validate_data(row [1], ''))

                    lat             = float(validate_data(row [6]))
                    long_           = float(validate_data(row [7]))
                    confirmed       = int(validate_data(row [3]))
                    deaths          = int(validate_data(row [4]))
                    recovered       = int(validate_data(row [5]))
                    active          = 0

                    # If there is a "," in the state, then it probably contains a county too
                    sep = state.find (",")
                    if sep > 0:
                        county = state[0:sep]
                        state = state [sep+2:]
                        #print ("|"+county+"|", "|"+state+"|")
                        state = translate_state (state)
                    
                    sep = state.find ("(From Diamond Princess)")
                    if sep > 0:
                        state = state [0:sep].strip()
                        state = translate_state (state)

                elif len(row_header) == 12:
                    county          = str(validate_data(row [1], ''))
                    state           = str(validate_data(row [2], ''))
                    country         = str(validate_data(row [3], ''))

                    lat             = float(validate_data(row [5]))
                    long_           = float(validate_data(row [6]))
                    confirmed       = int(validate_data(row [7]))
                    deaths          = int(validate_data(row [8]))
                    recovered       = int(validate_data(row [9]))
                    active          = int(validate_data(row [10]))

                # Get the Country ID                
                country_id = get_country_id (country, lat, long_)

                # Clean up the State data
                if state == "Unassigned Location":
                    state = ''
                
                if state == 'Chicago':
                    state = 'Illinois'

                if state == "OR " and country == "US":
                    state = translate_state ("OR")

                if state == "U.S." and country == "US":
                    state = ""
                
                if state == "United States Virgin Islands" and country == "US":
                    state = "Virgin Islands"

                if state == '' or state.isspace():
                    state_id = None
                    #state = country
                else:
                    state_id   = get_state_id   (state, lat, long_, country_id)

                if county == 'unassigned' or county == "Unassigned": 
                    county = ''

                county_id = None
                if state_id is None or county == '' or county.isspace():
                    county_id = None
                    #county = state
                else:
                    county_id  = get_county_id  (county, lat, long_, country_id, state_id)

                ################
                # Write the data
                ################

                # Get the current row from cur_rate3
                if not county_id is None:
                    sql_query_cur_rate_3 = ("SELECT cur_rate3_id, confirmed_cases, deaths, recovered_cases, active_cases FROM cur_rate3 WHERE d_country_id = %i and d_state_id = %i and d_county_id = %i ORDER BY cur_rate3_id DESC LIMIT 1") % (country_id, state_id, county_id)
                    cur_row = g_mydb.query_fetch_one (sql_query_cur_rate_3)
                elif not state_id is None:
                    sql_query_cur_rate_2 = ("SELECT cur_rate3_id, confirmed_cases, deaths, recovered_cases, active_cases FROM cur_rate3 WHERE d_country_id = %i and d_state_id = %i and d_county_id is null ORDER BY cur_rate3_id DESC LIMIT 1") % (country_id, state_id)
                    cur_row = g_mydb.query_fetch_one (sql_query_cur_rate_2)
                else:
                    sql_query_cur_rate_1 = ("SELECT cur_rate3_id, confirmed_cases, deaths, recovered_cases, active_cases FROM cur_rate3 WHERE d_country_id = %i and d_state_id is null and d_county_id is null ORDER BY cur_rate3_id DESC LIMIT 1") % (country_id)
                    cur_row = g_mydb.query_fetch_one (sql_query_cur_rate_1)
                
                cur_confirmed = 0
                cur_deaths    = 0
                cur_recovered = 0
                cur_active    = 0
                
                if cur_row is None:
                    # This is the first time.

                    # Insert row into cur_rate
                    if not county_id is None:
                        sql_insert_cur_rate_3 = ("INSERT INTO cur_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_country_id, d_state_id, d_county_id) VALUES (%i, %i, %i, %i, %i, %i, %i)") % (confirmed, deaths, recovered, active, country_id, state_id, county_id)
                        cur_row_id = g_mydb.execute_insert (sql_insert_cur_rate_3)
                    elif not state_id is None:
                        sql_insert_cur_rate_2 = ("INSERT INTO cur_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_country_id, d_state_id) VALUES (%i, %i, %i, %i, %i, %i)") % (confirmed, deaths, recovered, active, country_id, state_id)
                        cur_row_id = g_mydb.execute_insert (sql_insert_cur_rate_2)
                    else:
                        sql_insert_cur_rate_1 = ("INSERT INTO cur_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_country_id) VALUES (%i, %i, %i, %i, %i)") % (confirmed, deaths, recovered, active, country_id)
                        cur_row_id = g_mydb.execute_insert (sql_insert_cur_rate_1)
                else:
                    # Update the cur_rate table

                    cur_row_id    = int(validate_data(cur_row[0]))
                    cur_confirmed = int(validate_data(cur_row[1]))
                    cur_deaths    = int(validate_data(cur_row[2]))
                    cur_recovered = int(validate_data(cur_row[3]))
                    cur_active    = int(validate_data(cur_row[4]))

                    sql_update_cur_rate = ("UPDATE cur_rate3 SET confirmed_cases = %i, deaths = %i, recovered_cases = %i, active_cases = %i WHERE cur_rate3_id = %i") % (confirmed, deaths, recovered, active, cur_row_id)
                    g_mydb.execute_update(sql_update_cur_rate)

                # Generate the delta values
                delta_confirmed = confirmed - cur_confirmed
                delta_deaths    = deaths - cur_deaths
                delta_recovered = recovered - cur_recovered
                delta_active    = active - cur_active
                    
                # Write the f_rate entry
                if delta_confirmed !=0 or delta_deaths != 0 or delta_recovered !=0 or delta_active != 0:
                    # Query if the record is already there
                    if not county_id is None:
                        sql_query_f_rate_3 = ("SELECT f_rate3_id FROM f_rate3 WHERE d_country_id = %i and d_state_id = %i and d_county_id = %i and d_datetime_id = %i ORDER BY f_rate3_id DESC LIMIT 1") % (country_id, state_id, county_id, date_id)
                        f_row = g_mydb.query_fetch_one (sql_query_f_rate_3)
                    elif not state_id is None:
                        sql_query_f_rate_2 = ("SELECT f_rate3_id FROM f_rate3 WHERE d_country_id = %i and d_state_id = %i and d_county_id is null and d_datetime_id = %i ORDER BY f_rate3_id DESC LIMIT 1") % (country_id, state_id, date_id)
                        f_row = g_mydb.query_fetch_one (sql_query_f_rate_2)
                    else:
                        sql_query_f_rate_1 = ("SELECT f_rate3_id FROM f_rate3 WHERE d_country_id = %i and d_state_id is null and d_county_id is null and d_datetime_id = %i ORDER BY f_rate3_id DESC LIMIT 1") % (country_id, date_id)
                        f_row = g_mydb.query_fetch_one (sql_query_f_rate_1)

                    if f_row is None:
                        # Write a new record if there is a change (non-zero)
                        if not county_id is None:
                            sql_insert_f_rate_3 = ("INSERT INTO f_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_datetime_id, d_country_id, d_state_id, d_county_id) VALUES (%i, %i, %i, %i, %i, %i, %i, %i)") % (delta_confirmed, delta_deaths, delta_recovered, delta_active, date_id, country_id, state_id, county_id)
                            f_rate_id = g_mydb.execute_insert (sql_insert_f_rate_3)
                        elif not state_id is None:
                            sql_insert_f_rate_2 = ("INSERT INTO f_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_datetime_id, d_country_id, d_state_id) VALUES (%i, %i, %i, %i, %i, %i, %i)") % (delta_confirmed, delta_deaths, delta_recovered, delta_active, date_id, country_id, state_id)
                            f_rate_id = g_mydb.execute_insert (sql_insert_f_rate_2)
                        else:
                            sql_insert_f_rate_1 = ("INSERT INTO f_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_datetime_id, d_country_id) VALUES (%i, %i, %i, %i, %i, %i)") % (delta_confirmed, delta_deaths, delta_recovered, delta_active, date_id, country_id)
                            f_rate_id = g_mydb.execute_insert (sql_insert_f_rate_1)

                # Write the ts_rate entry
                # Query if the record is already there
                if not county_id is None:
                    sql_query_ts_rate_3 = ("SELECT ts_rate3_id FROM ts_rate3 WHERE d_country_id = %i and d_state_id = %i and d_county_id = %i and d_datetime_id = %i ORDER BY ts_rate3_id DESC LIMIT 1") % (country_id, state_id, county_id, date_id)
                    ts_row = g_mydb.query_fetch_one (sql_query_ts_rate_3)
                elif not state_id is None:
                    sql_query_ts_rate_2 = ("SELECT ts_rate3_id FROM ts_rate3 WHERE d_country_id = %i and d_state_id = %i and d_county_id is null and d_datetime_id = %i ORDER BY ts_rate3_id DESC LIMIT 1") % (country_id, state_id, date_id)
                    ts_row = g_mydb.query_fetch_one (sql_query_ts_rate_2)
                else:
                    sql_query_ts_rate_1 = ("SELECT ts_rate3_id FROM ts_rate3 WHERE d_country_id = %i and d_state_id is null and d_county_id is null and d_datetime_id = %i ORDER BY ts_rate3_id DESC LIMIT 1") % (country_id, date_id)
                    ts_row = g_mydb.query_fetch_one (sql_query_ts_rate_1)

                if ts_row is None:
                    if not county_id is None:
                        sql_insert_ts_rate_3 = ("INSERT INTO ts_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_datetime_id, d_country_id, d_state_id, d_county_id) VALUES (%i, %i, %i, %i, %i, %i, %i, %i)") % (confirmed, deaths, recovered, active, date_id, country_id, state_id, county_id)
                        ts_rate_id = g_mydb.execute_insert (sql_insert_ts_rate_3)
                    elif not state_id is None:
                        sql_insert_ts_rate_2 = ("INSERT INTO ts_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_datetime_id, d_country_id, d_state_id) VALUES (%i, %i, %i, %i, %i, %i, %i)") % (confirmed, deaths, recovered, active, date_id, country_id, state_id)
                        ts_rate_id = g_mydb.execute_insert (sql_insert_ts_rate_2)
                    else:
                        sql_insert_ts_rate_1 = ("INSERT INTO ts_rate3 (confirmed_cases, deaths, recovered_cases, active_cases, d_datetime_id, d_country_id) VALUES (%i, %i, %i, %i, %i, %i)") % (confirmed, deaths, recovered, active, date_id, country_id)
                        ts_rate_id = g_mydb.execute_insert (sql_insert_ts_rate_1)

                #print (county, county_id, state, state_id, country, country_id, last_update_str, lat, long_, confirmed, deaths, recovered, active)

                #if country_id == -1 or state_id == -1:
                #    print (file_name, state, country, country_id, state_id, lat, long_)


def load_history (source_directory):
    global g_mydb

    for file in sorted(os.listdir(source_directory)):
        if file.lower().endswith (".csv"):
            data_file = os.path.join(source_directory, file)
            file_date = file.replace(".csv","")
            print (data_file, file_date)
            load_data (data_file, file_date)

            g_mydb.commit()        

def main():
    global g_mydb

    g_mydb = mySql_DB()
    g_mydb.connect()

    load_history ("../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")
    
    g_mydb.disconnect()



if __name__ == '__main__':
    main()