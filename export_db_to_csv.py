import sqlite3
import csv

DB = "pricecharting.db"
OUT = "pricecharting_export.csv"

con = sqlite3.connect(DB)
cur = con.cursor()

cur.execute("SELECT * FROM cards")
cols = [d[0] for d in cur.description]
rows = cur.fetchall()

with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(cols)
    w.writerows(rows)

con.close()

print("Wrote", OUT)

