import sys
import logging
import utils as ut
import psycopg2


class Database:

    def __init__(self):
        # self.host = ''
        # self.username = ''
        # self.password = ''
        # self.port = '5432'
        # self.dbname = ''
        self.connkey = ''
        self.conn = None
        logging.basicConfig(level=logging.INFO)


    def open_connection(self):
        """Connect to a Postgres database."""
        try:
            if(self.conn is None):
                self.conn = ut.sql_connection(self.connkey)
        except psycopg2.DatabaseError as e:
            logging.error(e)
            sys.exit()
        finally:
            logging.info('Connection opened successfully.')

p1 = Database()
p1.connkey = 'aws_rds'
p1.open_connection() 