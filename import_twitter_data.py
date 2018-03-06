import sqlite3 as lite
import sys
from datetime import datetime
import os
import csv

def insert_rows(rows, cur):
    entries = []
    for row in rows:
        if len(row) != 10:
            continue
        username = row[0]
        t_time = datetime.strptime(row[1]+":00", "%Y-%m-%d %H:%M:%S")
        text = row[4]
        text = row[4].split(" # ")
        content = text[0]
        hashtag = "#"+", #".join(text[1:])
        retweet = int(row[2])
        likes = int(row[3])
        link = row[9]
        t_id = row[8]
        entries.append((t_time, username, content, hashtag, retweet, likes, link, t_id))
    cur.executemany('''INSERT INTO Twitter
                    (
                        time,
                        userName,
                        content,
                        hashtag,
                        retweet,
                        likes,
                        link,
                        id
                    )
                VALUES
                    (
                        ?,?,?,?,?,?,?,?
                    )
    ''', (entries))
    db.commit()

def upload_csv(csv_path, cur):
    print("Current file:", csv_path)
    with open(csv_path) as f:
        rows = f.readlines()
        rows =  csv.reader(rows[1:], quotechar='"', delimiter=";",
                           quoting=csv.QUOTE_ALL, skipinitialspace=True)
        insert_rows(rows, cur)

if __name__ == "__main__":

    db = lite.connect('sqlight.sqlite')
    db.commit()
    cur = db.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Twitter
                    (
                        time DATETIME,
                        userName TEXT,
                        content TEXT,
                        hashtag TEXT,
                        retweet INT,
                        likes INT,
                        link TEXT,
                        id TEXT
                    )''')

    twitter_root = "./twitter/"
    csv_files = os.listdir(twitter_root)
    for csv_file in csv_files:
        upload_csv(os.path.join(twitter_root, csv_file), cur)
