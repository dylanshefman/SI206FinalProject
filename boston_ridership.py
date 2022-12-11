# SI 206 Final Project
# Chicago vs. Boston: Delays in Metro Trains
# Dylan Shefman, Brian Metz, Bobby Housel

# current file: boston_ridership.py
# Gathering info about Boston ridership and adding it to database

from bs4 import BeautifulSoup
import requests, sqlite3

def get_ridership():
    url = 'https://en.wikipedia.org/wiki/Massachusetts_Bay_Transportation_Authority'
    resp = requests.get(url)

    # declare soups
    soup = BeautifulSoup(resp.content, "html.parser")
    table = soup.find("table", class_ = "wikitable")
    body = table.find("tbody")
    lines = body.find_all("a")
    values = body.find_all("b")
    
    total_ridership = 0
    riderships = []
    for i in range(len(values))[:4]:
        line_info = [i + 8, lines[i].text.strip(" Line"), int(values[i].text.replace(",",""))]
        riderships.append(line_info)
        total_ridership += int(values[i].text.replace(",",""))
    
    for i in range(len(riderships)):
        share = round(riderships[i][2] / total_ridership, 4) * 100
        riderships[i].append(share)

    return riderships

def write_line_id_table(cur, conn):
    lines = ["Red", "Green", "Orange", "Blue"]
    cur.execute("CREATE TABLE IF NOT EXISTS line_ids (name TEXT, id INTEGER PRIMARY KEY)")
    for i in range(len(lines)):
        cur.execute("INSERT OR IGNORE INTO line_ids (name, id) VALUES (?,?)", (f"b_{lines[i]}", i + 8))
    
    conn.commit()

def write_to_db(ridership_list, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Ridership (line_id INTEGER PRIMARY KEY, avg_wkdy_ridership INTEGER, share INTEGER)")
    for line in ridership_list:
        cur.execute("INSERT OR IGNORE INTO Ridership (line_id, avg_wkdy_ridership, share) VALUES (?,?,?)", (line[0], line[2], line[3]))
    
    # commit changes and close cursor
    conn.commit()
    cur.close()

def main():
    # declare connection and cursor
    conn = sqlite3.connect("chicago_vs_boston.db")
    cur = conn.cursor()

    write_line_id_table(cur, conn)

    ridership = get_ridership()
    write_to_db(ridership, cur, conn)

main()