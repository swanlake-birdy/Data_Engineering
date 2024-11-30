import requests
import pandas as pd
#create connect to database
from sqlalchemy import create_engine


#EXTRACT

def extract()-> dict:
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data


#result = extract()
#print(result)


#TRANSFORM

def transform(data:dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print(f"Total number of universities from ApI {len(data)}")
    df = df[df["name"].str.contains("California")]
    print(f"Number of universities in california {len(df)}")
    df['domains'] = [','.join(map(str,l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str,l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df [["domains","country", "web_pages","name"]]


#LOAD

def load(df:pd.DataFrame)-> None:
    """ Loads data into a sqllite database"""
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace')
    

# %%
data = extract()
df = transform(data)
load(df)
