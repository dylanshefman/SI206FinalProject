# SI 206 Final Project
# Chicago vs. Boston: Delays in Metro Trains
# Dylan Shefman, Brian Metz, Bobby Housel

# current file: visualization.py
# Extracting data from database and creating visualizations to represent it

import sqlite3
import matplotlib.pyplot as plt

def create_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS avg_offsets (line_id INTEGER PRIMARY KEY, avg_offset INTEGER)")
    conn.commit()

def write_avgs(cur, conn):
    cur.execute("SELECT id from line_ids")
    line_ids_tups = cur.fetchall()
    line_ids = []
    for line_id_tup in line_ids_tups:
        line_ids.append(line_id_tup[0])
    
    f = open("avg_offsets.txt", "w")

    # chicago
    for line_id in line_ids[:8]:
        cur.execute(f"SELECT offset FROM chicago WHERE line_id = '{line_id}'")
        tups = cur.fetchall()
        offsets = []
        for tup in tups:
            offsets.append(tup[0])
        
        avg_offset = round((sum(offsets) / len(offsets)), 2)
        
        cur.execute("INSERT OR IGNORE INTO avg_offsets (line_id, avg_offset) VALUES (?,?)", (line_id, avg_offset))
        f.write(f"{line_id}: {avg_offset}\n")
    
    # boston
    for line_id in line_ids[8:]:
        cur.execute(f"SELECT offset FROM boston WHERE line_id = '{line_id}'")
        tups = cur.fetchall()
        offsets = []
        for tup in tups:
            offsets.append(round(tup[0], 2))
        
        avg_offset = round((sum(offsets) / len(offsets)), 2)
        
        cur.execute("INSERT OR IGNORE INTO avg_offsets (line_id, avg_offset) VALUES (?,?)", (line_id, avg_offset))
        f.write(f"{line_id}: {avg_offset}\n")

    f.close()
    conn.commit()

def scatter(cur):
    cur.execute(
        '''
        SELECT avg_offsets.line_id, avg_offsets.avg_offset, Ridership.share
        FROM avg_offsets
        JOIN Ridership ON avg_offsets.line_id = Ridership.line_id
        '''
    )
    results = cur.fetchall()
    x,y,z=zip(*results[:8])
    plt.scatter(y,z, color = 'red', label="Chicago")

    x,y,z=zip(*results[8:])
    plt.scatter(y,z, color = 'green', label="Boston")
    plt.title("Average Offset vs. Ridership by Line in Chicago and Boston")
    plt.xlabel("Average Offset (sec)")
    plt.ylabel("Percentage of Ridership of Respective System")
    plt.legend(loc="lower left")
    plt.show()

def pie(cur):
    fig = plt.figure()
    ax1 = fig.add_subplot(121)
    ax2 = fig.add_subplot(122)

    # chicago
    cur.execute(
        '''
        SELECT line_ids.name, Ridership.avg_wkdy_ridership
        FROM line_ids
        JOIN Ridership ON line_ids.id = Ridership.line_id
        WHERE Ridership.line_id < 8
        '''
    )
    results = cur.fetchall()
    labels,values = zip(*results)
    
    labels = list(labels)
    for i in range(len(labels)):
        labels[i] = labels[i][2:]
    labels = tuple(labels)

    colors = ["blue", "brown", "green", "orange", "pink", "purple", "red", "yellow"]
    ax1.pie(values, labels=labels, colors=colors, shadow=True)

    # boston
    cur.execute(
        '''
        SELECT line_ids.name, Ridership.avg_wkdy_ridership
        FROM line_ids
        JOIN Ridership ON line_ids.id = Ridership.line_id
        WHERE Ridership.line_id >= 8
        '''
    )
    results = cur.fetchall()
    labels,values = zip(*results)
    
    labels = list(labels)
    for i in range(len(labels)):
        labels[i] = labels[i][2:]
    labels = tuple(labels)

    colors = ["red", "green", "orange", "blue"]
    ax2.pie(values, labels=labels, colors=colors, shadow=True, startangle=45)

    ax1.set_title("Chicago")
    ax2.set_title("Boston")
    plt.suptitle("Ridership by Line")
    plt.show()

def map(cur, conn):
    # create table for average offset at each station
    cur.execute("CREATE TABLE IF NOT EXISTS chicago_station_offsets (id INTEGER PRIMARY KEY, avg_offset INTEGER)")

    # add data to table
    cur.execute("SELECT id FROM chicago_stations")
    stations = cur.fetchall()
    for station_tup in stations:
        sum_offsets = 0
        count = 0
        for station in station_tup:
            cur.execute(f"SELECT offset from chicago WHERE station_id = '{station}'")
            trip_tups = cur.fetchall()
            for trip in trip_tups:
                sum_offsets += trip[0]
            avg_offset = round(sum_offsets / len(trip_tups))
            cur.execute("INSERT OR IGNORE INTO chicago_station_offsets (id, avg_offset) VALUES (?,?)", (station, avg_offset))
    
    # commit changes
    conn.commit()

    # select info from database
    cur.execute(
        '''
        SELECT chicago_stations.line_id, chicago_stations.name, chicago_stations.lat, chicago_stations.lon, chicago_station_offsets.avg_offset
        FROM chicago_stations
        JOIN chicago_station_offsets ON chicago_stations.id = chicago_station_offsets.id
        '''
    )
    station_data = cur.fetchall()
    line_ids,names,lats,lons,avg_offsets = zip(*station_data)
    
    # make figure and declare subplots
    fig = plt.figure(figsize=(15,6))
    full = fig.add_subplot(121)
    inset = fig.add_subplot(122)

    # make full-system map
    for i in range(len(line_ids)):
        if avg_offsets[i] >= 1450:
            color = ("red")
        elif avg_offsets[i] >= 1100:
            color = ("orange")
        elif avg_offsets[i] >= 750:
            color = ("yellow")
        else:
            color = ("green")

        lon = (lons[i])
        lat = (lats[i])
        full.scatter(x=lon, y=lat, color=color, marker=".")
        full.axes.xaxis.set_ticklabels([])
        full.axes.yaxis.set_ticklabels([])

    # make inset
    for i in range(len(line_ids)):
        if lons[i] > -315700 and lats[i] < 151000 and lats[i] > 150600:
            if avg_offsets[i] >= 1450:
                color = ("red")
            elif avg_offsets[i] >= 1100:
                color = ("orange")
            elif avg_offsets[i] >= 750:
                color = ("yellow")
            else:
                color = ("green")

            lon = (lons[i])
            lat = (lats[i])
            inset.scatter(x=lon, y=lat, color=color, marker=".")
            inset.axes.xaxis.set_ticklabels([])
            inset.axes.yaxis.set_ticklabels([])

    full.set_title('Full "L" System')
    inset.set_title("Inset (Downtown and Nearby)")
    plt.suptitle("Average Offset at CTA Stations")
    plt.show()

def main():
    # declare connection and cursor
    conn = sqlite3.connect(f"chicago_vs_boston.db")
    cur = conn.cursor()

    create_table(cur, conn)
    write_avgs(cur, conn)
    scatter(cur)
    pie(cur)
    map(cur, conn)

    # close cursor
    cur.close()

main()