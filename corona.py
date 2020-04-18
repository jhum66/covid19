import sys
import getopt
import requests
import time
import datetime
import mysql.connector

data_all_url = 'https://services1.arcgis.com/0MSEUqKaxRlEPj5g/ArcGIS/rest/services/Coronavirus_2019_nCoV_Cases/FeatureServer/1/query?where=Confirmed+%3E+0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=OBJECTID%2C+Province_State%2C+Country_Region%2C+Last_Update%2C+Lat%2C+Long_++%2CConfirmed%2C+Deaths%2C+Recovered&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=Confirmed+desc&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=standard&f=pjson&token='
data_all_ur1 = 'https://services1.arcgis.com/0MSEUqKaxRlEPj5g/ArcGIS/rest/services/Coronavirus_2019_nCoV_Cases/FeatureServer/1/query?where=Confirmed+%3E+0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=OBJECTID%2C+Admin2%2C+Province_State%2C+Country_Region%2C+Last_Update%2C+Lat%2C+Long_++%2CConfirmed%2C+Deaths%2C+Recovered&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=Confirmed+desc&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=standard&f=pjson&token='
data_us_url  = 'https://services1.arcgis.com/0MSEUqKaxRlEPj5g/ArcGIS/rest/services/Coronavirus_2019_nCoV_Cases/FeatureServer/1/query?where=Country_Region+%3D+%27US%27&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=OBJECTID%2C+Province_State%2C+Country_Region%2C+Last_Update%2C+Lat%2C+Long_++%2CConfirmed%2C+Deaths%2C+Recovered&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=Confirmed+desc&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=standard&f=pjson&token='

search_parm = ''
search_arg = ''
my_list = []

def validate_data (value, dv = 0):
    if not value:
        value = dv
    return value

def read_data (data_url):
    global my_list

    try:
        r = requests.get(data_url)

        myjson = r.json()
        total_confirmed = 0
        total_deaths = 0
        total_recovered = 0

        #print (myjson)
        for feature in myjson["features"]:
            #print (feature)
            attributes = feature ["attributes"]
            country = attributes ["Country_Region"]
            last_update = attributes ["Last_Update"]
            
            #print (last_update)
            if last_update:
                last_update = int (last_update) / 1000
                last_update_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_update))
            
            province_state = validate_data (attributes ["Province_State"],"")
            confirmed      = validate_data (attributes ["Confirmed"])
            deaths         = validate_data (attributes ["Deaths"])
            recovered      = validate_data (attributes ["Recovered"])
            lat            = validate_data (attributes ["Lat"])
            long_          = validate_data (attributes ["Long_"])

            data = {'country'     : country,
                    'country_id'  : -1,
                    'state'       : province_state,
                    'state_id'    : -1,
                    'last_update' : last_update,
                    'update_dt'   : last_update_str,
                    'update_id'   : -1,
                    'confirmed'   : confirmed,
                    'deaths'      : deaths,
                    'recovered'   : recovered,
                    'lat'         : lat,
                    'long'        : long_
                   }

            my_list.append(data)
    
            total_confirmed += confirmed
            total_deaths += deaths
            total_recovered += recovered

            #if search_parm == 'u':
                #print (province_state, search_arg)

            if (search_parm == 'u' and province_state == search_arg) or (search_parm == 'c' and country == search_arg) :
                print (country + "," + province_state + "," + str(confirmed) + "," + str(deaths) + "," + str(recovered) + "," + last_update_str)

        print ("Totals: ", total_confirmed, total_deaths, total_recovered)

    except requests.exceptions.HTTPError as err:
        print ("Error calling data service ")
        print (err)

'''
mySQL functions
'''
mysql_db = ""

def mysql_connect ():
    global mysql_db

    # Connect to mySQL database
    mysql_db = mysql.connector.connect (host="localhost", user="virus", passwd="corona", database="coronavirus", auth_plugin="mysql_native_password")

def mysql_disconnect ():
    global mysql_db

    mysql_db.commit()
    mysql_db.close()

def execute_insert (insert_ddl):
    global mysql_db
        
    cursor = mysql_db.cursor()
    # print (insert_ddl)
    cursor.execute (insert_ddl)

    last_id = cursor.lastrowid
    cursor.close()

    return last_id

def execute_update (update_ddl):
    global mysql_db
        
    cursor = mysql_db.cursor()
    cursor.execute (update_ddl)

    cursor.close()

def query_fetch_one (query_string):
    global mysql_db
        
    cursor = mysql_db.cursor()
    cursor.execute (query_string)

    cursor_row = cursor.fetchone()
    cursor.close()

    return cursor_row

'''
End mySql Functions
'''

def process_country (country, lat, long_):
    country_id = -1

    # If None, assign value of Unknown
    if not country:
        print (country)
        country = "Unknown"

    # Remove single quotes
    country = country.replace("'","\\'")

    # Check if this country is in d_country table
    # print (country, state)
    sql_query_d_country = ("SELECT d_country_id FROM d_country WHERE name = '%s'") % (country)
    cursor_rows = query_fetch_one (sql_query_d_country)

    if not cursor_rows is None:
        country_id = cursor_rows[0]
    else:
        # Not found; insert a row for the country
        sql_insertd_country = ("INSERT INTO d_country (name, lat, long_) VALUES ('%s', %f, %f)") % (country, lat, long_)
        country_id = execute_insert (sql_insertd_country)

    return country_id

def process_state (country, state, country_id, lat, long_):
    state_id = -1

    # If None, assign value of the country
    if not state:
        #print (country, state)
        state = country

    # Remove single quotes
    state = state.replace("'","\\'")

    # Check if this state is in d_state table
    sql_query_d_state = ("SELECT d_state_id FROM d_state WHERE name = '%s' AND d_country_id = %i") % (state, country_id)
    cursor_rows = query_fetch_one (sql_query_d_state)

    if not cursor_rows is None:
        state_id = cursor_rows[0]
    else:
        # Not found; insert a row for the state
        sql_insert_d_state = ("INSERT INTO d_state (name, lat, long_, d_country_id) VALUES ('%s', %f, %f, %i)") % (state, lat, long_, country_id)
        state_id = execute_insert (sql_insert_d_state)

    return state_id

def process_datetime (update_dt):
    datetime_id = -1

    # If None, assign value of current date time
    if not update_dt:
        from datetime import datetime
        dt = datetime.now()
        update_dt = dt.strftime('%Y-%m-%d %H:%M:%S')

    # Check if this state is in d_state table
    sql_query_d_datetime = ("SELECT d_datetime_id FROM d_datetime WHERE update_dt = '%s' ORDER BY d_datetime_id DESC LIMIT 1") % (update_dt)
    cursor_rows = query_fetch_one (sql_query_d_datetime)

    if not cursor_rows is None:
        datetime_id = cursor_rows[0]
    else:
        # Not found; insert a row for the country
        sql_insert_d_datetime = ("INSERT INTO d_datetime (update_dt) VALUES ('%s')") % (update_dt)

        datetime_id = execute_insert (sql_insert_d_datetime)

    return datetime_id

def check_if_data_is_new (country_id, state_id, confirmed, deaths, recovered):
    bad_data = False
    new_values = (confirmed, deaths, recovered)

    # Query the last row from the cur_rate table
    # Select f.f_rate_id, f.confirmed_cases, f.deaths, f.recovered_cases, d.update_dt, timestampdiff(second, d.update_dt, now()) as time_diff, now() as now from f_rate f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id  where f.d_country_id = 5 and f.d_state_id = 5 order by f.f_rate_id desc;
    # query = ("Select f.f_rate_id, f.confirmed_cases, f.deaths, f.recovered_cases, d.update_dt, timestampdiff(second, d.update_dt, now()) as time_diff from f_rate f inner join d_datetime d on f.d_datetime_id = d.d_datetime_id where d_country_id = %i and d_state_id = %i order by f_rate_id desc limit 1") % (country_id, state_id)
    sql_query_cur_rate = ("SELECT cur_rate_id, confirmed_cases, deaths, recovered_cases FROM cur_rate WHERE d_country_id = %i and d_state_id = %i ORDER BY cur_rate_id DESC LIMIT 1") % (country_id, state_id)
    cursor_rows = query_fetch_one(sql_query_cur_rate)

    if not cursor_rows is None:
        db_cur_rate_id = int(cursor_rows[0])
        db_confirmed   = int(cursor_rows[1])
        db_deaths      = int(cursor_rows[2])
        db_recovered   = int(cursor_rows[3])

        new_values = (confirmed - db_confirmed, deaths - db_deaths, recovered - db_recovered)

        # Check if the values are positive
        # Otherwise ignore the update
        if new_values [0] >= 0 and new_values [1] >= 0 and new_values [2] >= 0:
            # Update the table cur_rate
            sql_update_cur_rate = ("UPDATE cur_rate SET confirmed_cases = %d, deaths = %d, recovered_cases = %d WHERE cur_rate_id = %d") % (confirmed, deaths, recovered, db_cur_rate_id)
            execute_update (sql_update_cur_rate)
        else:
            new_values = (0, 0, 0)
            bad_data = True
    else:
        # Row not in cur_rate
        # Insert a new row
        sql_insert_cur_rate = ("INSERT INTO cur_rate (d_country_id, d_state_id, confirmed_cases, deaths, recovered_cases) VALUES (%d, %d, %d, %d, %d)") % (country_id, state_id, confirmed, deaths, recovered)
        execute_insert (sql_insert_cur_rate)

    # Check if we should write this to the timeseries
    new_data = False
    sql_query_ts_rate = ("SELECT f.ts_rate_id, f.confirmed_cases, f.deaths, f.recovered_cases, d.update_dt, TIMESTAMPDIFF(second, d.update_dt, NOW()) AS time_diff FROM ts_rate f INNER JOIN d_datetime d ON f.d_datetime_id = d.d_datetime_id WHERE d_country_id = %i AND d_state_id = %i ORDER BY ts_rate_id DESC LIMIT 1") % (country_id, state_id)
    cursor_rows = query_fetch_one (sql_query_ts_rate)

    if not cursor_rows is None:
        db_confirmed   = int(cursor_rows[1])
        db_deaths      = int(cursor_rows[2])
        db_recovered   = int(cursor_rows[3])
        db_time_secs   = int(cursor_rows[5])

        if (confirmed == db_confirmed and deaths == db_deaths and recovered == db_recovered):
            new_data = False

            if db_time_secs > 86400: 
                # More than one day old
                new_data = True
        else:
            new_data = True
    else:
        new_data = True
        
    return (new_values, new_data, bad_data)

def process_rates (country_id, state_id, time_id, confirmed, deaths, recovered, o_confirmed, o_deaths, o_recovered):
    f_id = -1
    db_time_id = -1

    #print ("process_rates", country_id, state_id, time_id, confirmed, deaths, recovered, o_confirmed, o_deaths, o_recovered)
    # Check the last time_id from the f_rate table
    sql_query_1_f_rate = ("SELECT f_rate_id, confirmed_cases, deaths, recovered_cases, d_datetime_id FROM f_rate WHERE d_country_id = %i and d_state_id = %i ORDER BY f_rate_id DESC LIMIT 1") % (country_id, state_id)
    cursor_rows = query_fetch_one (sql_query_1_f_rate)

    if not cursor_rows is None:
        db_time_id = int(cursor_rows[4])

    if db_time_id != time_id:
        # Check if the record exists
        sql_query_2_f_rate = ("SELECT f_rate_id FROM f_rate WHERE d_country_id = %i and d_state_id = %i and d_datetime_id = %i ORDER BY f_rate_id DESC LIMIT 1") % (country_id, state_id, time_id)
        cursor_rows = query_fetch_one (sql_query_2_f_rate)

        if cursor_rows is None:
            # Record does not exists

            # Insert the data
            sql_insert_f_rate = ("INSERT INTO f_rate (d_country_id, d_state_id, d_datetime_id, confirmed_cases, deaths, recovered_cases) VALUES (%d, %d, %d, %d, %d, %d)") % (country_id, state_id, time_id, confirmed, deaths, recovered)
            f_id = execute_insert (sql_insert_f_rate)
        else:
            # From the table
            f_id = int (cursor_rows[0])

    return f_id

def update_data ():
    global my_list

    # Connect to mySQL database
    mysql_connect()
    ####  = mysql.connector.connect (host="localhost", user="virus", passwd="corona", database="coronavirus", auth_plugin="mysql_native_password")

    for data in my_list:
        country_id = -1
        state_id   = -1
        time_id    = -1
        f_id       = -1
        country    = str(data ["country"])
        state      = str(data ["state"])
        update_dt  = str(data ["update_dt"])
        lat        = float(data ["lat"])
        long_      = float(data ["long"])
        country_id = process_country (country, lat, long_)
        state_id   = process_state (country, state, country_id, lat, long_)

        process_data = True
        #process_data = False
        #if country_id == 5 and state_id == 5:
        #    process_data = True

        if process_data:
            # Get the counts
            confirmed = int (data["confirmed"])
            deaths    = int (data["deaths"])
            recovered = int (data["recovered"])

            # Check if we have new data
            new_data = True

            new_values_tmp = check_if_data_is_new (country_id, state_id, confirmed, deaths, recovered)
            new_values     = new_values_tmp [0]
            new_data       = new_values_tmp [1]
            bad_data       = new_values_tmp [2]

            # Insert into the data
            if not bad_data:
                time_id = process_datetime (update_dt)

                if new_values [0] != 0 or new_values [1] != 0 or new_values [2] != 0:             
                    f_id = process_rates (country_id, state_id, time_id, new_values [0], new_values [1], new_values [2], confirmed, deaths, recovered)
                #else:
                #    print ("Not added", country_id, state_id, time_id, new_values [0], new_values [1], new_values [2])

                if new_data:
                    # Write a record to the time series
                    sql_insert_ts_rate = ("INSERT INTO ts_rate (d_country_id, d_state_id, d_datetime_id, confirmed_cases, deaths, recovered_cases) VALUES (%d, %d, %d, %d, %d, %d)") % (country_id, state_id, time_id, confirmed, deaths, recovered)
                    execute_insert (sql_insert_ts_rate)

    # Close the connection
    mysql_disconnect()

def main ():
    global my_list
    global search_parm
    global search_arg

    try:
        opts, args = getopt.getopt(sys.argv[1:],"hu:c:d:",["state=","country=","data="])

    except getopt.GetoptError:
        print (sys.argv[0] + ' -h -u <state> -c <country> -d <data_frequency>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print (sys.argv[0] + ' -h -u <state> -c <country> -d <data frequency>')
            sys.exit()
        elif opt in ("-u", "--state"):
            search_parm = "u"
            search_arg = arg
            read_data (data_us_url)
            #update_data ()
        elif opt in ("-c", "--country"):
            search_parm = "c"
            search_arg = arg
            read_data (data_all_url)
            #update_data ()
        elif opt in ("-d", "--data"):
            search_parm = "d"
            search_arg = int(arg)
            while True:
                my_list = []
                from datetime import datetime
                print (str(datetime.now()))
                read_data (data_all_url)
                update_data ()
                time.sleep (search_arg)

if __name__ == '__main__':
    main()
