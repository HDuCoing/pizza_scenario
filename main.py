import snowflake.connector
import pandas as pd
from datetime import datetime, date, timedelta
import numpy as np



# CLI requirement : pip install "snowflake-connector-python[pandas]"

# Data has already been loaded into snowflake - so connecting remotely here to it now.
conn = snowflake.connector.connect(
    user='***',
    password='***',
    account='***',
    database='PIZZA_SAMPLE',
    schema='SAMPLE'
    )

conn.cursor().execute("USE DATABASE PIZZA_SAMPLE")


def fetch_pandas(cur, sql):
    # uses the snowflake cursor to execute sql on snowflake account
    cur.execute(sql)
    while True:
        # fetches 60k rows then breaks after the max (54kish) data is fetched
        dat = cur.fetchmany(60000)
        if not dat:
            break
        # puts rows into a dataframe for continued relational data
        df = pd.DataFrame(dat, columns=[i[0] for i in cur.description])

        df['T_DATE'] = pd.to_datetime(df['T_DATE'])
        # create a new df for the info we want
        newdf = pd.DataFrame(columns=['SHOP_ID', 'STATUS', 'LOWER_RANGE', 'UPPER_RANGE'])
        df.sort_values(by='SHOP_ID', axis=0, ascending=True, inplace=True)
        # check each row
        for index, row in df.iterrows():
            # increment 1 day
            nextday = row['T_DATE'] + timedelta(days=1)
            # counts how many days in a row its closed
            closedcount = 0
            # goes one day at a time, for 30 days
            for days in range(1,31):
                nextday = nextday+timedelta(days=1)
                if nextday not in df.values and row['SHOP_ID'] in df.values:
                    closedcount += 1
                    if closedcount >= 30:
                        # add status as closed and upper_range as the date its closed
                        newdf.loc[len(newdf.index)] = [row['SHOP_ID'], "Closed", row['T_DATE'], nextday]
                else:
                    closedcount = 0
                    newdf.loc[len(newdf.index)] = [row['SHOP_ID'], "Open", row['T_DATE'], pd.NA]
            print(newdf)

sql_query = 'SELECT SHOP_ID, T_DATE, N_TRANS ' \
            'FROM "PIZZA_SAMPLE"."SAMPLE"."PIZZA"'

fetch_pandas(cur=conn.cursor(), sql=sql_query)