import sqlite3

DB_PATH = "pricecharting.db"

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS set_meta (
        set_slug TEXT PRIMARY KEY,
        pc_list_a_name TEXT,
        pokellector_set_name TEXT,
        pokellector_url TEXT,
        base_total INTEGER,
        secret_total INTEGER,
        released_md TEXT,
        released_year INTEGER,
        released_raw TEXT,
        language TEXT
    );
    """)

    con.commit()
    con.close()
    print("Created clean set_meta table.")

if __name__ == "__main__":
    main()

