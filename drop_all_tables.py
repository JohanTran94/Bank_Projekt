from db import Db

def drop_all_tables():
    # Get a connection from the singleton Db class
    conn = Db().get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    # Drop the entire 'public' schema including all tables, views, etc.
    cursor.execute("DROP SCHEMA public CASCADE;")

    # Recreate the 'public' schema
    cursor.execute("CREATE SCHEMA public;")

    # Restore default privileges (optional but recommended)
    cursor.execute("GRANT ALL ON SCHEMA public TO postgres;")
    cursor.execute("GRANT ALL ON SCHEMA public TO public;")

    print("🟢 All tables in the 'public' schema have been dropped.")

    # Close cursor and connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    drop_all_tables()
