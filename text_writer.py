import csv
from datetime import datetime
import os.path
import pandas as pd
import sqlite3

def write_to_db(**numbers):
    gatorlink_percent = "{0:.2f}%".format((float(numbers['gatorlink_count'])/float(numbers['author_count'])) * 100)
    ufentity_percent = "{0:.2f}%".format((float(numbers['ufentity_count'])/float(numbers['author_count'])) * 100)
    ufcurrententity_percent = "{0:.2f}%".format((float(numbers['ufcurrententity_count'])/float(numbers['author_count'])) * 100)
    doi_percent = "{0:.2f}%".format((float(numbers['doi_count'])/float(numbers['publication_count'])) * 100)
    pmid_percent = "{0:.2f}%".format((float(numbers['pmid_count'])/float(numbers['publication_count'])) * 100)
    doi_pmid_percent = "{0:.2f}%".format((float(numbers['doi_pmid_count'])/float(numbers['publication_count'])) * 100)

    conn = sqlite3.connect('metrics.db')
    c = conn.cursor()

    c.execute('''create table if not exists metric_log
                    (date TEXT, authors INT, gatorlinks INT, UFEntities INT, UFCurrentEntities INT, publications INT, dois INT, pmids INT, doi_and_pmid INT, orphan_pubs INT, publishers INT, journals INT)''')

    timestamp = datetime.now().strftime("%Y_%m_%d")
    metrics = (timestamp, int(numbers['author_count']), int(numbers['gatorlink_count']), int(numbers['ufentity_count']),
                int(numbers['ufcurrententity_count']), int(numbers['publication_count']), int(numbers['doi_count']), int(numbers['pmid_count']),
                int(numbers['doi_pmid_count']), int(numbers['orphan_count']), int(numbers['publisher_count']), int(numbers['journal_count']))

    c.execute('INSERT INTO metric_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (metrics))

    print(pd.read_sql_query('SELECT * FROM metric_log', conn))

    conn.commit()
    conn.close()

def write_to_csv():
    conn = sqlite3.connect('metrics.db')
    c = conn.cursor()
    c.execute('''SELECT * FROM metric_log ORDER BY date DESC''')
    rows = c.fetchall()
    names = [description[0] for description in c.description]

    with open('metrics.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(names)
        writer.writerows(rows)