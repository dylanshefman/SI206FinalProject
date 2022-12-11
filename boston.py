# SI 206 Final Project
# Chicago vs. Boston: Delays in Metro Trains
# Dylan Shefman, Brian Metz, Bobby Housel

# current file: boston.py
# Adding Boston train timing information to database

import requests
import sqlite3
import re
import math
import datetime as dt

# SECTION 1: utility
API_KEY = "wX9NwuHnZU2ToO7GmGR9uw"

def deg_to_secs(deg):
    '''
    Converts degrees of longitude/latitude to seconds.
    Seconds are more usable than degrees, as slight differences will be more noticeable.
    One second is equal to ~80 ft.
    '''
    return deg * 3600

class Station:
    '''
    Represents a single station of the four MBTA lines in consideration.
    '''
    def __init__(self, id, name, color, lat, long, dest):
        self.id = id
        self.name = name
        self.color = color
        self.lat = lat
        self.long = long
        self.dest = dest

class Trip:
    '''
    Represents a trip made between two adjacent stations on one of the four MBTA lines.
    '''
    def __init__(self, line, station1, station2, prd, act, offset):
        self.line = line
        self.station1 = station1
        self.station2 = station2
        self.prd = prd
        self.act = act
        self.offset = offset

# SECTION 2: core
def get_stations_by_color():
    '''
    Extracts train station ids from text file containing train and bus stop information.
    Each line in the text file indicates one train or bus stop.
    A line indicates a train station if it begins with 7 and includes 'RapidTransit'.
    This function refers to rail line as 'color' to distinguish it from 'line', which refers to a line of the text file.
    '''
    stations = {}

    with open("boston_stops.txt") as station_info:
        
        # iterate through lines of text file
        for line in station_info.readlines():
            
            # if given stop is train station, collect information about it
            if line[0] == "7" and re.search("RapidTransit", line):
                info_list = line.split(",")
                station_id = int(info_list[0])
                name = info_list[2]
                color = info_list[3].split(" - ")[1].rstrip("Line").strip()
                lat = deg_to_secs(float(re.findall("42\.\d+", line)[0]))
                long = deg_to_secs(float(re.findall("-7[01]\.\d+", line)[0]))
                dest = info_list[3].split(" - ")[-1]

                # ignore Mattapan trolley stations and drop-off only stations
                if color == "Mattapan Trolley" or dest == "Drop-off Only": continue
                # consider all '& north'/'& west' destinations equal
                if re.search("& \w{4,5}", dest):
                    dest = re.findall("& (\w{4,5})", dest)[0]
                # fix destination for last stops on each route
                if dest == "Exit Only":
                    dest = info_list[2]
                # remove branch from destination if it exists
                if re.search("\(\w\)", dest):
                    dest = dest[4:]
                # remove 'from' statement from destination
                if "from" in dest:
                    dest = dest.split(" ")[0]
                
                # use collected information to declare instance of Station class
                station_obj = Station(station_id, name, color, lat, long, dest)

                # if we have not yet come across color, declare dict
                if stations.get(color) is None:
                    stations[color] = {}
                # if we have not yet come across destination within color, declare list
                if stations[color].get(dest) is None:
                    stations[color][dest] = []
                # append station to dictionary
                stations[color][dest].append(station_obj)

    return stations

def get_station_pairs(stations_by_line):
    '''
    Returns list of pairs of adjacent stations.
    The MBTA API requires the user to input two stations; this function prepares the data for use in API call.
    '''
    # declare list
    pairs = []
    
    # iterate through destinations within given color
    for dest in stations_by_line:
        inner = stations_by_line[dest]

        # ignore destinations with only 1-2 stations
        if len(inner) < 3: continue

        # sort list in order of descending ids
        inner.sort(key=lambda x: x.id, reverse=True)

        # iterate through station list within given destination
        for i in range(len(inner) - 1):
            # create pair and add it to list
            pairs.append((inner[i], inner[i + 1]))

    return pairs

def get_url(station1, station2):
    '''
    Creates URL for API request.
    '''
    now = round(dt.datetime.timestamp(dt.datetime.now()))
        # current time
    day_ago = now - 86400
        # one week prior to current time

    return f"https://performanceapi.mbta.com/developer/api/v2.1/traveltimes?api_key={API_KEY}&format=json&from_stop={station1.id}&to_stop={station2.id}&from_datetime={day_ago}&to_datetime={now}"
    
def get_trips_from_url(url, cur):
    '''
    Requests API for timing information related to given URL, which corresponds to a single trip between two adjacent stations.
    Cleans data and returns list of Trip objects
    '''
    # get trip info
    trip_info = requests.get(url).json()["travel_times"]

    # declare list
    trips = []

    # iterate through trips returned by API
    for trip in trip_info:
        prd = int(trip["benchmark_travel_time_sec"])
        act = int(trip["travel_time_sec"])
        line = trip["route_id"]
        offset = abs(prd - act)

        # fix line branching
        if "-" in line:
            line = line.split("-")[0]

        # find station ids
        station1 = re.findall("from_stop=(\d{5})", url)[0]
        station2 = re.findall("to_stop=(\d{5})", url)[0]

        # get line id
        line = "b_" + line
        cur.execute(f"SELECT id from line_ids WHERE name = '{line}'")
        line_id = cur.fetchone()[0]

        # use data collected to declare instance of Trip class
        trip_obj = Trip(line_id, station1, station2, prd, act, offset)
        trips.append(trip_obj)
    
    return trips

def create_table(cur, conn):
    '''
    Creates table in database.
    '''
    cur.execute("CREATE TABLE IF NOT EXISTS boston (line_id INTEGER, station1_id INTEGER, station2_id INTEGER, prd_time INTEGER, arr_time INTEGER, offset INTEGER)")
    
    # commit changes
    conn.commit()

def write_to_db(trip, cur, conn):
    '''
    Writes given trip object to database.
    '''
    info_tup = (trip.line, trip.station1, trip.station2, trip.prd, trip.act, trip.offset)
    cur.execute("INSERT OR IGNORE INTO boston (line_id, station1_id, station2_id, prd_time, arr_time, offset) VALUES (?,?,?,?,?,?)", info_tup)
    
    # commit changes
    conn.commit()

# SECTION 3: main    
def main():
    # get dictionary of station objects organized by line and destination
    stations = get_stations_by_color()

    # declare connection and cursor
    conn = sqlite3.connect("chicago_vs_boston.db")
    cur = conn.cursor()

    # create table
    create_table(cur, conn)

    all_trips = []

    # iterate through lines
    for line in stations:
        # get station pairs
        pairs = get_station_pairs(stations[line])
        # append each of given line's pairs to all_pairs list
        for pair in pairs:
            # get URL representing pair
            url = get_url(pair[0], pair[1])
            print(url)
            # get list of trips representing pair
            trips_of_pair = get_trips_from_url(url, cur)
            # append each trip object to greater list
            for trip in trips_of_pair:
                all_trips.append(trip)
    
    num_rows = len(cur.execute("SELECT * FROM boston").fetchall())
    
    # write each trip in list to database
    for trip in all_trips[num_rows:num_rows + 25]:
        write_to_db(trip, cur, conn)

    
    # close cursor
    cur.close()



main()