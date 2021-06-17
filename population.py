import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime

from sqlalchemy import create_engine


def load_data(db_cursor, mydb):
    print('\n\n----- Loading data from CSV to Pandas DataFrame')
    # downsized dataset is used due to the limited computing resources.
    # resample py script downscales the usage.csv file by the desired fraction.
    # name can be changed to usage.csv for full data load.
    usage_data = pd.read_csv('usage_downsized.csv', names=['cust_id', 'event_start', 'event_type', 'rate_plan_id', 'flag_1', 'flag_2', 'duration', 'charge', 'month'])

    print('Initial shape:', usage_data.shape)

    print('\n---- Dropping month column')
    usage_data = usage_data.drop(['month'], axis = 1)

    print('\n---- Dropping duplicate rows')
    usage_data = usage_data.drop_duplicates()

    print('\n---- Validating data')
    # validate data as needed, for example, check if the event type belongs to one of 4 existing classes:
    event_type_values = usage_data['event_type'].unique()
    for val in event_type_values:
        if val != 'VOICE' and val != 'DATA' and val != 'SMS' and val != 'MMS':
            # in such case, error has to be handled, either by removing the record that is not acceptable and
            # alert the provider about the inconsistent data, or expand the list of possible values and allow such record.
            print('Error. Unexpected event type found. Population is stopped.')
            print(val)
            exit()

    # also check for damaged data, such as columns containing null values.
    missing_perc = usage_data.isnull().sum()*100/len(usage_data)
    if max(missing_perc) > 0:
        # in such case, error is raised and damaged data has to be handled as required. for example, the
        # empty cells have to be filled with values or the rows with empty values removed. The provider
        # has to be alerted of poor quality data.
        print('Error. Null values found in the dataset. Population is stopped.')
        exit()

    # some other data validation should be performed, such as checking whether the input values are within the
    # expected range. for example, check whether charge values do not exceed the existing greatest value. if
    # the value is greater than all the stored values, it might indicate an error within data.



    # add data upload time stamp so that it could be used later on to remove data after 6 months
    # due to the legal requirements.
    usage_data['load_time'] = datetime.now().strftime("%Y-%m-%d")

    print('Shape after duplicates removal:',usage_data.shape)

    db_cursor.execute("CREATE DATABASE telcoDWH")

    db_cursor.execute("USE telcoDWH")

    db_cursor.execute("SHOW TABLES")

    # if tables are not created, create the three tables
    if ('usage_data',) not in db_cursor:

        usage_data_table = '''CREATE TABLE usage_data (
           cust_id INT NOT NULL,
           event_start DATETIME NOT NULL,
           event_type CHAR(10) NOT NULL,
           rate_plan_id INT NOT NULL,
           flag_1 INT NOT NULL,
           flag_2 INT NOT NULL,
           duration INT NOT NULL,
           charge FLOAT NOT NULL,
           load_time DATETIME NOT NULL
        )'''
        db_cursor.execute(usage_data_table)

        # creating table for service type distribution
        type_distribution_table ='''CREATE TABLE type_distribution (
           event_type CHAR(10) PRIMARY KEY,
           project_count INT,
           customers INT
        )'''
        db_cursor.execute(type_distribution_table)

        # creating table for rate plan distribution
        rate_distribution_table ='''CREATE TABLE rate_distribution (
           rate_plan_id INT PRIMARY KEY,
           project_count INT,
           customers INT
        )'''
        db_cursor.execute(rate_distribution_table)

    db_cursor.execute("SHOW TABLES")
    print('\ncreated tables:')
    for table in db_cursor:
        print(table)


    print('\n---- Loading usage_data initial database')
    # sqlalchemy engine is used to load Pandas dataframe to MySQL database
    engine = create_engine("mysql://root:@localhost/telcoDWH")
    con = engine.connect()
    usage_data.to_sql(name='usage_data',con=con,if_exists='replace')



    print('\n---- Creating and loading type_distribution Data Mart')
    # creating a table type_distribution, where project distribution for each type
    # is calculated by counting number of rows for each distinct event type.
    # number of distinct customers is also calculated for rows containing each
    # event type. dataframe is built to be uploaded to database.
    event_types = usage_data['event_type'].unique()
    projects_for_event = usage_data['event_type'].value_counts()
    no_customers = []
    for event in event_types:
        no_customers.append((usage_data.loc[usage_data['event_type']==event])['cust_id'].nunique())

    type_distribution = pd.DataFrame({'event_type': event_types,
                                      'project_count':projects_for_event,
                                      'customers':no_customers})

    type_distribution.to_sql(name='type_distribution',con=con,if_exists='replace')



    print('\n---- Creating and loading rate_distribution Data Mart\n')
    # creating table rate_distribution, where project distribution for each rate plan
    # is calculated by counting number of rows for each distinct rate plan.
    # number of distinct customers is also calculated for rows containing different
    # rate plan. dataframe is built to be uploaded to database.
    rate_plans = usage_data['rate_plan_id'].unique()
    project_for_plan = usage_data['rate_plan_id'].value_counts()
    no_customers = []
    for plan in rate_plans:
        no_customers.append((usage_data.loc[usage_data['rate_plan_id']==plan])['cust_id'].nunique())

    rate_distribution = pd.DataFrame({'rate_plan_id': rate_plans,
                                      'project_count':project_for_plan,
                                      'customers':no_customers})

    rate_distribution.to_sql(name='rate_distribution',con=con,if_exists='replace')

    con.close()




# connecting to mysql
mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = ''
)


# initializing database cursor for SQL operations
db_cursor = mydb.cursor()
db_cursor.execute("SHOW DATABASES")

# if population is initial, the database is created named 'telcoDWH'
if ('telcoDWH',) not in db_cursor:
    print('\nWarehouse was not initialized. The creation begins.')
    load_data(db_cursor, mydb)

else:
    print('The data was already loaded to database')
