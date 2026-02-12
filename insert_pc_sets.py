import csv
import sqlite3

DB_PATH = "pricecharting.db"
CSV_PATH = "pricecharting_sets.csv"  
# columns: set_slug, pc_list_a_name

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cur.execute("""
            INSERT OR IGNORE INTO set_meta (set_slug, pc_list_a_name)
            VALUES (?, ?)
            """, (row["set_slug"], row["set_name"]))

    con.commit()
    con.close()
    print("Inserted PC sets.")

if __name__ == "__main__":
    main()

