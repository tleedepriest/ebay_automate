import sqlite3

DB_PATH = "pricecharting.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("""
UPDATE set_meta
SET base_total = 159
WHERE set_slug = 'pokemon-journey-together'
""")

con.commit()
con.close()

print("Updated Journey Together base_total to 159")

