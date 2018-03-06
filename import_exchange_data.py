import sqlite3 as lite
import sys
from datetime import datetime
import os

def insert_exchange(rows, coin_name, cur):
    entries = []
    for row in rows:
        row = row.strip().split(",")
        if row[0].find(".") == -1:
            row[0] = row[0] + ".000"
        trade_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
        volume = abs(float(row[1]))
        price = float(row[2])
        trade_type = "BUY" if float(row[1]) > 0 else "SELL"
        entries.append((trade_time, coin_name, volume, price, trade_type))
    cur.executemany('''INSERT INTO Exchange
                    (
                        time,
                        coinName,
                        volume,
                        price,
                        tradeType
                    )
                VALUES
                    (
                        ?,?,?,?,?
                    )
    ''', (entries))
    db.commit()

def upload_exchange(csv_path, coin_name, cur):
    btc_files = os.listdir(csv_path)
    for file in btc_files:
        print("Current file:", file)
        with open(os.path.join(csv_path, file)) as f:
            rows = f.readlines()
            insert_exchange(rows[1:], coin_name, cur)

if __name__ == "__main__":

    db = lite.connect('sqlight.sqlite')
    db.commit()
    cur = db.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Exchange
                    (
                        time DATETIME,
                        coinName TEXT,
                        volume FLOAT,
                        price FLOAT,
                        tradeType TEXT
                    )''')

    upload_exchange("./bitfinex/BTC", "BTC", cur)
    upload_exchange("./bitfinex/LTC", "LTC", cur)

