import sys
import getopt
import requests
import time
import datetime
import mysql.connector
import csv

mydb = ""

def execute_insert (insert_ddl):
    global mydb
        
    cursor = mydb.cursor()
    #print (insert_ddl)
    cursor.execute (insert_ddl)

    last_id = cursor.lastrowid
    cursor.close()

    return last_id

def query_fetch_one (query_string):
    global mydb
        
    cursor = mydb.cursor()
    cursor.execute (query_string)

    cursor_row = cursor.fetchone()
    cursor.close()

    return cursor_row

def validate_data (value, dv = 0):
    if not value:
        value = dv
    return value

def get_country_id (country):
    country_id = -1

    # De-reference single quotes
    country_tmp = country.replace ("'", "\\'")
    query_d_country =("SELECT d_country_id FROM d_country WHERE name='%s'") % (country_tmp)
    return_row = query_fetch_one (query_d_country)

    if not return_row is None:
        country_id = return_row [0]
    #else:
    #    print ("Country ID not found for: " + country)

    return country_id

def get_state_id (state, lat, long_, country_id):
    state_id = -1

    # De-reference single quotes
    state_tmp = state.replace ("'", "\\'")
    query_d_state  = ("SELECT d_state_id FROM d_state WHERE name='%s' and d_country_id=%i") % (state_tmp, country_id)
    insert_d_state = ("INSERT INTO d_state (name, lat, long_, d_country_id) VALUES ('%s', %f, %f, %i)") % (state, lat, long_, country_id)
    
    return_row = query_fetch_one (query_d_state)

    if not return_row is None:
        state_id = return_row [0]
    else:
        state_id = execute_insert (insert_d_state)

    return state_id

def get_datetime_id (update_dt):
    datetime_id = -1

    query_d_datetime  = ("SELECT d_datetime_id FROM d_datetime WHERE update_dt='%s'") % (update_dt)
    insert_d_datetime = ("INSERT INTO d_datetime (update_dt) VALUES ('%s')") % (update_dt)
    
    #print (query_d_datetime)
    #print (insert_d_datetime)
    return_row = query_fetch_one (query_d_datetime)

    if not return_row is None:
        datetime_id = return_row [0]
    else:
        datetime_id = execute_insert (insert_d_datetime)

    #print (datetime_id)
    return datetime_id

def insert_ts_rate2 (data_rec):
    country_id = data_rec["country_id"]
    state_id   = data_rec["state_id"]
    date_id    = data_rec["date_id"]
    confirmed  = int(validate_data(data_rec["confirmed_cases"]))
    deaths     = int(validate_data(data_rec["deaths"]))
    recovered  = int(validate_data(data_rec["recovered_cases"]))

    query_ts_rate2  = ("SELECT ts_rate2_id from ts_rate2 WHERE d_country_id = %i and d_state_id = %i and d_datetime_id = %i order by ts_rate2_id desc limit 1") % (country_id, state_id, date_id)
    insert_ts_rate2 = ("INSERT INTO ts_rate2 (d_country_id, d_state_id, d_datetime_id, confirmed_cases, deaths, recovered_cases) values (%d, %d, %d, %d, %d, %d)") % (country_id, state_id, date_id, confirmed, deaths, recovered)
    #print (insert_ts_rate2)

    # Check if the record exists
    return_row = query_fetch_one (query_ts_rate2)

    if return_row is None:
        ts_id = execute_insert (insert_ts_rate2)
    else:
        ts_id = int(validate_data(return_row[0]))

    return ts_id

def insert_f_rate2 (data_rec):
    country_id = data_rec["country_id"]
    state_id   = data_rec["state_id"]
    date_id    = data_rec["date_id"]
    confirmed  = int(validate_data(data_rec["confirmed_cases"]))
    deaths     = int(validate_data(data_rec["deaths"]))
    recovered  = int(validate_data(data_rec["recovered_cases"]))

    query_f_rate2  = ("SELECT f_rate2_id from f_rate2 WHERE d_country_id = %i and d_state_id = %i and d_datetime_id = %i order by f_rate2_id desc limit 1") % (country_id, state_id, date_id)
    insert_f_rate2 = ("INSERT INTO f_rate2 (d_country_id, d_state_id, d_datetime_id, confirmed_cases, deaths, recovered_cases) values (%d, %d, %d, %d, %d, %d)") % (country_id, state_id, date_id, confirmed, deaths, recovered)
    #print (insert_ts_rate2)

    # Check if the record exists
    return_row = query_fetch_one (query_f_rate2)

    if return_row is None:
        f_id = execute_insert (insert_f_rate2)
    else:
        f_id = int(validate_data(return_row[0]))

    return f_id

def load_data (file_name, data_list, combined, combined_fact, col_name):
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
                for i in range (4 , len (row_header), 1):
                    column_name = str (row_header [i])
                    from datetime import datetime 
                    new_column_name = str(datetime.strptime(column_name,'%m/%d/%y'))
                    new_date_id = get_datetime_id (new_column_name)
                    date_id.update([(column_name, new_date_id)])
                    # print (date_id)
            else:
                state   = str(validate_data(row [0], ''))
                country = str(validate_data(row [1], ''))
                lat     = float(validate_data(row [2]))
                long_   = float(validate_data(row [3]))

                if state == '' or state.isspace():
                    state = country

                country_id = get_country_id (country)
                state_id   = get_state_id (state, lat, long_, country_id)

                if country_id == -1 or state_id == -1:
                    print (file_name, state, country, country_id, state_id, lat, long_)

                # Process the data columns
                previous_day_val = 0
                for i in range (4 , len (row_header), 1):
                    cur_day_val      = int(validate_data(row[i]))
                    fact_val         = cur_day_val - previous_day_val
                    previous_day_val = cur_day_val
                    
                    # Check if key is in Combined
                    combined_key = str(country_id)+"-"+str(state_id)+"-"+str(date_id [row_header[i]])
                    
                    if combined_key in combined:
                        new_data = combined[combined_key]
                        new_data [col_name] = cur_day_val
                    else:
                        # Add the key
                        new_data = {'country_id'      : country_id,
                                    'state_id'        : state_id,
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
    from datetime import datetime
    print (str(datetime.now()), " Started ts_rate2")

    for key, value in combined.items():
        #print (key, value)

        ts_id = insert_ts_rate2 (value)
        # print (ts_id)
    
    from datetime import datetime
    print (str(datetime.now()), " Finished ts_rate2")

    for key, value in combined_fact.items():
        f_id = insert_f_rate2 (value)
        #print (f_id)
        #if value ['country_id'] == 5:
        #    print (key, value)

    from datetime import datetime
    print (str(datetime.now()), " Finished f_rate2")


def main ():
    global mydb

    combined      = {}
    combined_fact = {}
    confirmed     = []
    deaths        = []
    recovered     = []

    # Connect to mySQL database
    mydb = mysql.connector.connect (host="localhost", user="virus", passwd="corona", database="coronavirus", auth_plugin="mysql_native_password")

    load_data ("time_series_covid19_confirmed_global.csv", confirmed, combined, combined_fact, 'confirmed_cases')
    load_data ("time_series_covid19_deaths_global.csv", deaths, combined, combined_fact, 'deaths')
    load_data ("time_series_covid19_recovered_global.csv", recovered, combined, combined_fact, 'recovered_cases')

    insert_data (combined, combined_fact)
    
    # Close the connection
    mydb.commit()
    mydb.close()


if __name__ == '__main__':
    main()
