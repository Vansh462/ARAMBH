# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

df=pd.read_excel("best solution till now (attempt 2)/NBFCsandARCs10012023 (5).XLSX")

def safe_correction(x):
  try:
    return int(x)-1
  except:
    return 0

df['SR No.']=df['SR No.'].apply(lambda x: safe_correction(x))

from googlesearch import search


def result(df,bank_name,index):
    index=int(index)
    query = f"{bank_name} official site"

    if not pd.isna(df['NBFC Name'][index]):
        try:
            for url in search(query,num_results=1):
                df.at[index, 'Official Website'] = url
                print(f"Bank at index {index} found \n")
                return
        except Exception as e:
            print(f"An error occurred at index {index}: {e} \n")
            if "429" in str(e):
                df.at[index, 'Official Website'] ="too many requests error 429"
                return 
    elif pd.isna(df['NBFC Name'][index]):
        print(f"couldn't find index {index} due to missing values \n")        
    return

from concurrent.futures import ThreadPoolExecutor

#method - 0.26 s/ search
def process_banks(df, column_name='NBFC Name', result_column='Official Website'):
    df[result_column] = ''

    with ThreadPoolExecutor(max_workers=10) as executor:
        try:

            args = [(df, row[column_name], index) for index, row in df.iterrows()]

            list(executor.map(lambda p: result(*p), args))

        except Exception as e:
            print(f"Stopping execution due to error: {e} \n")

    return


process_banks(df)


#saving output file
df2=pd.DataFrame()
df2['Regional Office']=df['Regional Office']
df2['NBFC Name']=df['NBFC Name']
df2['Address']=df['Address']
df2['Email ID']=df['Email ID']
df2['Official Website']=df['Official Website']

df2.to_excel('best_solution_till_now_(attempt 2)/outputx.xlsx',index=False)

