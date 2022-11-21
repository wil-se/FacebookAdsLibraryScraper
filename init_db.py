import sqlite3

con = sqlite3.connect('found2.db', check_same_thread=False)
cur = con.cursor()

def init_db():

    # # Records
    # #   0      1         2           3       4          5          6            7            8           9
    # # (id, product, original_url, price, currency, sells_count, tot_sold, last_sell, date_created, discovery_date)
    # cur.execute("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, product TEXT UNIQUE, original_url TEXT UNIQUE, price REAL, currency TEXT, sells_count INTEGER, tot_sold REAL, last_sell datetime, date_created datetime, discovery_date datetime default current_timestamp, active INTEGER default 1)")

    # # Blacklist
    # cur.execute("CREATE TABLE IF NOT EXISTS blacklist(id INTEGER PRIMARY KEY, product_url TEXT UNIQUE)")

    cur.execute("CREATE TABLE IF NOT EXISTS filters(id INTEGER PRIMARY KEY, active_ads INTEGER, date_from TEXT, date_to TEXT, country TEXT, keywords TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS products(adid INTEGER PRIMARY KEY, active_ads INTEGER, country TEXT, adtitle TEXT, keyword TEXT, pageurl TEXT, adsonpage TEXT, adurl TEXT, creation_time TEXT, tag TEXT)")


init_db()
print('done')