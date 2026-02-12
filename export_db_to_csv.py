import sys
import sqlite3
import csv

def main(table_name):
    DB = "pricecharting.db"
    OUT = f"pricecharting_export_{table_name}.csv"

    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.execute(f"SELECT * FROM {table_name}")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    con.close()

    print("Wrote", OUT)

if __name__ == "__main__":
    main(sys.argv[1])

