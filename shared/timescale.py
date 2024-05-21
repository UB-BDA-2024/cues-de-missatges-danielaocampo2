import psycopg2
import os


class Timescale:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.cursor = self.conn.cursor()
        
    def getCursor(self):
            return self.cursor
    
    def enable_autocommit(self, enable=True):
        """ Enable or disable autocommit mode on the connection. """
        self.conn.autocommit = enable

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def ping(self):
        return self.conn.ping()
    
    def execute(self, query,  params=None):
       return self.cursor.execute(query, params)
        
    def fetch_all(self, query, params=None):
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def fetch_one(self, query, params=None):
        self.cursor.execute(query, params)
        return self.cursor.fetchone() 
    
    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()