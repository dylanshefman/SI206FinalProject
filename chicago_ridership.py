# SI 206 Final Project
# Chicago vs. Boston: Delays in Metro Trains
# Dylan Shefman, Brian Metz, Bobby Housel

# current file: chicago_ridership.py
# Gathering info about Chicago ridership and adding it to database

from bs4 import BeautifulSoup
import requests, sqlite3

class Line:
    def __init__(self):
        self.name = ""
        self.ridership = 0
        self.share = 0


def get_ridership():
    '''
    Scrapes Wikipedia page for ridership statistics per line.
    Returns list of Line instances, one per line.
    '''
    # Define the URL of the Wikipedia page
    url = 'https://en.wikipedia.org/wiki/List_of_Chicago_"L"_stations'

    # Send a request to the URL and store the response
    response = requests.get(url)

    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, "html.parser")
    tbody = soup.find("tbody")
    rows = tbody.find_all("tr")

    # declare variables
    total_ridership = 0
    lines = []
    line_id = 0

    # iterate through row soups
    for row in rows[1:]:

        # find line name and ridership
        name = row.find("a").text
        ridership = int(row.find_all("td")[-1].text.strip().replace(",", ""))
        total_ridership += ridership

        # store in list
        lines.append([line_id, ridership, 0])

        # increment id
        line_id += 1
    
    # set each line's share of total ridership
    for i in range(len(lines)):
        share = round((lines[i][1] / total_ridership), 4) * 100
        lines[i][-1] = share
    
    return lines

def write_line_id_table(cur, conn):
    lines = ["Blue", "Brown", "Green", "Orange", "Pink", "Purple", "Red", "Yellow"]
    cur.execute("CREATE TABLE IF NOT EXISTS line_ids (name TEXT, id INTEGER PRIMARY KEY)")
    for i in range(len(lines)):
        cur.execute("INSERT OR IGNORE INTO line_ids (name, id) VALUES (?,?)", (f"c_{lines[i]}", i))
    
    conn.commit()

def write_to_db(ridership_list, cur, conn):
    '''
    Writes data contained in each Line object in list to database.
    '''
    
    # write to database
    cur.execute("CREATE TABLE IF NOT EXISTS Ridership (line_id INTEGER PRIMARY KEY, avg_wkdy_ridership INTEGER, share INTEGER)")
    for line in ridership_list:
        cur.execute("INSERT OR IGNORE INTO Ridership (line_id, avg_wkdy_ridership, share) VALUES (?,?,?)", (line))
    
    # commit changes and close cursor
    conn.commit()
    cur.close()


def main():
    # declare variables
    conn = sqlite3.connect("chicago_vs_boston.db")
    cur = conn.cursor()
    
    write_line_id_table(cur, conn)

    ridership = get_ridership()
    write_to_db(ridership, cur, conn)
    



main()


