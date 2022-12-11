# SI 206 Final Project
# Chicago vs. Boston: Delays in Metro Trains
# Dylan Shefman, Brian Metz, Bobby Housel

# current file: chicago.py
# Adding Chicago train timing information to database

import requests
import sqlite3
import re

# SECTION 1: utility

API_KEY = "6349f324180e485da072a01cdf8c36be"

def condense(day, hr, min, sec):
    '''
    Condenses time in day/hr/min/sec format to number of seconds since beginning of month.
    '''
    iday = int(day) * 86400 
    ihr = int(hr) * 3600
    imin = int(min) * 60
    isec = int(sec)
    return (iday + ihr + imin + isec)

def deg_to_secs(deg):
    '''
    Converts degrees of longitude/latitude to seconds.
    Seconds are more usable than degrees, as slight differences will be more noticeable.
    One second is equal to ~80 ft.
    '''
    return deg * 3600

# SECTION 2: core
def get_stations():
    '''
    Extracts train station ids and locations from text file containing train and bus stop information.
    Train station ids are 5 digits and begin with "4".
    '''
    stations = []

    with open("chicago_stops.txt") as station_info:
        for line in station_info.readlines():
            id = line.split(",")[0]

            # if given stop is train station, collect info about it
            if len(id) == 5 and id[0] == "4":
                name = line.split(",")[2].strip('"')
                # remove parentheses from station names that include them
                if name.endswith(")"):
                    name = " ".join(name.split("(")[:-1]).strip()

                lat = float(line.split(",")[4])
                lon = float(line.split(",")[5])

                # set line_id to arbitrary placeholder value - updated later
                line_id = 50

                info_tup = (int(id), line_id, name, round(deg_to_secs(lat)), round(deg_to_secs(lon)))
                stations.append(info_tup)

    return stations

def get_url(station_id):
    '''
    Creates URL for API request.
    '''
    # set result limit to 25
    max_results = str(25)

    # return customized link
    return f"http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx?key={API_KEY}&mapid={station_id}&outputType=JSON&max={max_results}"

def get_timing_from_url(url, cur):
    '''
    Requests API for timing information for given URL corresponding to a single station.
    Cleans data and returns list of tuples in format (line, station_id, train_num, prd_time, arr_time, offset).
    '''
    timing_list = []
    
    # get station info
    station_info = requests.get(url).json()

    # iterate through trains arriving into given station
    for train in station_info["ctatt"]["eta"]:
        
        # get train-specific info
        train_num = train["rn"]
        line = train["rt"]

        # turn route IDs into route names
        if line == "G":
            line = "Green"
        elif line == "P":
            line = "Purple"
        elif line == "Org":
            line = "Orange"
        elif line == "Brn":
            line = "Brown"
        elif line == "Y":
            line = "Yellow"

        # get predicted arrival date/time
        prd = train["prdt"]
        pdate = prd.split("T")[0]
        pday = pdate.split("-")[2]
        ptime = prd.split("T")[1]
        phr = ptime.split(":")[0]
        pmin = ptime.split(":")[1]
        psec = ptime.split(":")[2]
        prd_condensed = condense(pday, phr, pmin, psec)

        # get actual arrival date/time
        arr = train["arrT"]
        adate = arr.split("T")[0]
        aday = adate.split("-")[2]
        atime = arr.split("T")[1]
        ahr = atime.split(":")[0]
        amin = atime.split(":")[1]
        asec = atime.split(":")[2]
        arr_condensed = condense(aday, ahr, amin, asec)

        # calculate offset
        offset = abs(arr_condensed - prd_condensed)

        # get line id
        line = "c_" + line
        cur.execute(f"SELECT id from line_ids WHERE name = '{line}'")
        line_id = cur.fetchone()[0]

        # add line id to database
        station_id = re.findall("mapid=(\d{5})", url)[0]
        cur.execute(f"UPDATE chicago_stations SET line_id = {line_id} WHERE id = {station_id}")

        # put it all together
        station_id = train["staId"]
        timing_tup = (line_id, station_id, train_num, prd_condensed, arr_condensed, offset)
        timing_list.append(timing_tup)
    
    return timing_list

def create_tables(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS chicago (line_id INTEGER, station_id INTEGER, train_num INTEGER, prd_time INTEGER, arr_time INTEGER, offset INTEGER, PRIMARY KEY (station_id, train_num))")
    cur.execute("CREATE TABLE IF NOT EXISTS chicago_stations (id INTEGER PRIMARY KEY, line_id INTEGER, name TEXT, lat INTEGER, lon INTEGER)")

    conn.commit()

def write_timing_to_db(timing_tup, cur, conn):
    '''
    Writes given list of tuples corresponding to a single station to database.
    '''
    # create table if it does not exist
    cur.execute("CREATE TABLE IF NOT EXISTS chicago (line_id INTEGER, station_id INTEGER, train_num INTEGER, prd_time INTEGER, arr_time INTEGER, offset INTEGER, PRIMARY KEY (station_id, train_num))")

    # add tuple to table
    cur.execute("INSERT OR IGNORE INTO chicago (line_id, station_id, train_num, prd_time, arr_time, offset) VALUES (?,?,?,?,?,?)", timing_tup)
    
    # commit changes
    conn.commit()

def write_location_to_db(tup, cur, conn):
    '''
    Writes given tuples corresponding to a station's location to database.
    '''
    cur.execute("CREATE TABLE IF NOT EXISTS chicago_stations (id INTEGER PRIMARY KEY, line_id INTEGER, name TEXT, lat INTEGER, lon INTEGER)")

    # add tuple to table
    cur.execute("INSERT OR IGNORE INTO chicago_stations (id, line_id, name, lat, lon) VALUES (?,?,?,?,?)", tup)

    # commit changes
    conn.commit()

# SECTION 3: main
def main():
    # get list of station ids
    stations = get_stations()

    # declare connection and cursor
    conn = sqlite3.connect("chicago_vs_boston.db")
    cur = conn.cursor()

    # create tables
    create_tables(cur, conn)

    timing_tups = []
    # iterate through station id list
    for station in stations:
        # write location information to database
        write_location_to_db(station, cur, conn)
        # create url
        url = get_url(station[0])
        # generate timing info for all trains arriving into given station
        timing = get_timing_from_url(url, cur)
        # append each train's info tuple of a given line to greater list
        for entry in timing:
            timing_tups.append(entry)
        
    
    # add max 25 items at a time
    cur.execute("SELECT * FROM chicago")
    num_rows = len(cur.fetchall())
    try:
        for timing in timing_tups[num_rows:num_rows + 25]:
            write_timing_to_db(timing, cur, conn)
        print("25 rows written to database")
    except:
        print("All data written to database")
    
    # close cursor
    cur.close()



main()