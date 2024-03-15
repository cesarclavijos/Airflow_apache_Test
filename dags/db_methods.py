import os
import requests
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
import sqlalchemy as sa

class db_methods:
    
    @classmethod
    def create_engie(self):
        url = URL.create(
        drivername='redshift+redshift_connector',
        host = os.environ.get('DB_HOST'),                         
        port ='5439', 
        database = os.environ.get('DB_DBNAME'),
        username = os.environ.get('DB_USER'),
        password = os.environ.get('DB_PASSWORD') 
        )
        engine = sa.create_engine(url)
        return engine
        

    @classmethod
    def get_data_db(self):
        engine = self.create_engie()
        engine.execute(text("SELECT * FROM currency")).fetchall()

    @classmethod
    def inser_data(self, df2):
        sql_engine = self.create_engie()
        df2.to_sql(name='currency', con=sql_engine,schema='cesar_hcs_coderhouse', if_exists='append', index= False)