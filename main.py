import snowflake.connector
import pandas as pd
from datetime import timedelta



# CLI requirement : pip install "snowflake-connector-python[pandas]"

# Data has already been loaded into snowflake - so connecting remotely here to it now.
conn = snowflake.connector.connect(
    user='*',
    password='*',
    account='*',
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
        #print(sql_translated_code(df))
        print(df)


def sql_translated_code(df):
    # Almost the same as the pure SQL query but i create the dataframe differently
    df['T_DATE'] = pd.to_datetime(df['T_DATE'])

    # shifts the dates
    df['NEXT_T_DATE'] = df.groupby('SHOP_ID')['T_DATE'].shift(-1)
    df['NEXT_T_DATE'] = pd.to_datetime(df['NEXT_T_DATE'])

    # calculates the number of days between next date and current date
    df['NUM_DAYS'] = (df['NEXT_T_DATE'] - df['T_DATE']).dt.days

    # creates a dataframe for closed stores only
    closed_df = df[(df['NUM_DAYS'] - 2) >= 30].copy()
    closed_df['T_DATE'] = closed_df['T_DATE'] + timedelta(days=1)
    closed_df['NEXT_T_DATE'] = closed_df['NEXT_T_DATE'] - timedelta(days=1)
    closed_df['STATUS'] = 'CLOSED'

    # creates a dataframe for open stores
    open_df = df[(df['NUM_DAYS'] - 2) < 30].copy()
    open_df['STATUS'] = 'OPEN'
    # renaming for output
    closed_df.rename(columns={'T_DATE': 'LOWER_RANGE', 'NEXT_T_DATE': 'UPPER_RANGE'}, inplace=True)
    open_df.rename(columns={'T_DATE': 'LOWER_RANGE', 'NEXT_T_DATE': 'UPPER_RANGE'}, inplace=True)

    # concatenate the dataframes together
    result_df = pd.concat([
        closed_df[['SHOP_ID', 'STATUS', 'LOWER_RANGE', 'UPPER_RANGE']],
        open_df[['SHOP_ID', 'STATUS', 'LOWER_RANGE', 'UPPER_RANGE']]
    ])
    print(result_df)


sql_query = 'select SHOP_ID, T_DATE, N_TRANS ' \
            'from "PIZZA_SAMPLE"."SAMPLE"."PIZZA"'
'''
Creates virtual tables that select the shop and the transaction date, it uses
lag and lead to look at a "window of time" day by day for 30 days. Similar to the python code.
Looks at the difference of the previous day and the following day and puts into a column.
Closed is determined by evaluating the next 30 days of data to see if there were transactions
'''
pure_sql_solution ="""WITH cte
as (
Select shop_id, t_date, lead(t_date) over(partition by shop_id order by t_date asc) as next_t_date, 
datediff(day, t_date, next_t_date) as num_days from "PIZZA_SAMPLE"."SAMPLE"."PIZZA"), 
closed as (select shop_id, dateadd(day, 1, t_date) as t_date, dateadd(day, -1, next_t_date) as 
next_t_date from cte where (num_days-2) >= 30), 
open as (select shop_id, t_date, next_t_date from cte where (num_days-2) < 30)
Select shop_id, 'CLOSED' as status, t_date as lower_range, next_t_date as upper_range from closed 
union select shop_id, 'OPEN' as status, t_date, next_t_date from open"""

fetch_pandas(cur=conn.cursor(), sql=pure_sql_solution)