from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os
# uid='postgre'
# pwd='0000'
uid='user1'
pwd='user1'

server='localhost'
db='AdventureWorks'
port='5432'
dir=r'D:\\Code\\ETL\\ETL-Python-Scripts\\Data'

# loading the data into Postgresql Database

def load(df,tbl):
    try:
        rows_imported=0
        engine=create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}...')

        # loading in Postresql
        df.to_sql(f'stg_{tbl}',engine,if_exists='replace',index=False)
        print("Success in data loading")
    except Exception as e:
        print("Error in loading data to postresql error is: "+str(e))



# extract data from excel files

def extract():
    try:
        directory=dir
        for filename in os.listdir(directory):
            file_no_ext=os.path.splitext(filename)[0]
            if(filename.endswith('.xlsx')):
                f=os.path.join(directory,filename)
                if os.path.isfile(f):
                    df=pd.read_excel(f)
                    # leading into DB
                    load(df,file_no_ext)
    except Exception as e:
        print("Data Extract Error is: "+str(e))


# main code

try: 
    df=extract()
except Exception as e:
    print('Error while extracting data: error is '+str(e))