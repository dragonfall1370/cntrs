import datetime as dt
import boto3

from os import truncate
import findspark
from traitlets.traitlets import Integer
findspark.init()

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from common import *
import datetime
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
import pyodbc
import tabulate
import pandas as pd
import ast
from common import vincere_common
# import rds_struct

from sshtunnel import SSHTunnelForwarder #Run pip install sshtunnel
from sqlalchemy.orm import sessionmaker #Run pip install sqlalchemy
import json

# Classic PyGreSQL
from pg import DB
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


schemas=['bi_warehouse']

tables=['tenant_info']


staging_pw= "Tm9fQ52AHQCzNCEVgf"
playground_pw= "Q4x4GgBvbgFxcxnBqf"
production_pw= "xF5m5yFacSkpDxSVnf"

prod_rds = "23gKhAueVQUNjmac8H"

for sch in schemas:
    for tab in tables:
        print(sch, tab)
        with SSHTunnelForwarder(
            ('monitor.perxtech.net', 22), #Remote server IP and SSH port
            ssh_username = "long",
            ssh_password = "",
            ssh_pkey="~/.ssh/id_rsa",
            ssh_private_key_password="Longden0710",
            remote_bind_address=('platformdb.perxtech.net', 5432),
            local_bind_address=('localhost', 15432)
        ) as server: #PostgreSQL server IP and sever port on remote machine
            server.start() #start ssh sever
            print ('Server connected via SSH')

            #connect to PostgreSQL

            local_port = str(server.local_bind_port)
            engine = create_engine("""postgresql+psycopg2://long:{pw}@127.0.0.1:{local_port}/platform_production""".format(pw=prod_rds, local_port=local_port),  pool_size=100, max_overflow=200, client_encoding='utf8', executemany_mode='batch')

            Session = sessionmaker(bind=engine)
            session = Session()

            query = """select * from starhub.new_user_tier_current_cycles nutcc"""

            df = pd.read_sql(
                query, session.bind
            )

            with SSHTunnelForwarder(
            ('monitor.perxtech.org', 22), #Remote server IP and SSH port
            ssh_username = "long",
            ssh_password = "",
            ssh_pkey="~/.ssh/id_rsa",
            ssh_private_key_password="Longden0710",
            remote_bind_address=('redshift.perxtech.org', 5439),
            local_bind_address=('localhost', 15438)
            )   as server2: #PostgreSQL server IP and sever port on remote machine

                server2.start() #start ssh sever
                print ('Server2 connected via SSH')

                #connect to PostgreSQL

                local_port2 = str(server2.local_bind_port)
                engine2 = create_engine("""postgresql+psycopg2://long:{pw}@127.0.0.1:{local_port}/oltp_transactions""".format(pw=playground_pw, local_port=local_port2),  pool_size=100, max_overflow=200, client_encoding='utf8', executemany_mode='batch')

                Session2 = sessionmaker(bind=engine2)
                session2 = Session2()

                df.to_sql(name='loyalty_tier_history', schema='long', con=session2.bind, if_exists='replace', index=False, method='multi', chunksize=20000)

                session2.close()
                server2.stop()
                server2.close()

            session.close()
            server.stop()
            server.close()

