# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

df=pd.read_excel("/content/NBFCsandARCs10012023 (5).XLSX")

def safe_correction(x):
  try:
    return int(x)-1
  except:
    return 0

df['SR No.']=df['SR No.'].apply(lambda x: safe_correction(x))
df.head()

from googlesearch import search
import time

class TooManyRequestsError(Exception):
    pass

def result(df,bank_name,index):
    index=int(index)
    query = f"{bank_name} official site"

    try:
        for url in search(query,num=1):
          df.at[index, 'Official Website'] = url
          print(f"Bank at index {index} found \n")
          return
    except Exception as e:
        print(f"An error occurred at index {index}: {e}")
        if "429" in str(e):
          raise TooManyRequestsError(e)
          return None

    return

#method - 0.26 s/ search
def process_banks(df, column_name='NBFC Name', result_column='Official Website'):
    df[result_column] = ''

    with ThreadPoolExecutor(max_workers=10) as executor:
        try:

            args = [(df, row[column_name], index) for index, row in df.iterrows()]

            list(executor.map(lambda p: result(*p), args))

        except TooManyRequestsError as e:
            print(f"Stopping execution due to error: {e}")

    return

from concurrent.futures import ThreadPoolExecutor
def process_banks(bank_names, delay=2):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
        for i in range(0, len(bank_names), 100):  # Process in batches of 100
            batch = bank_names[i:i + 100]
            batch_results = list(executor.map(result, batch))
            results.extend(batch_results)
            print(f"Processed batch {i // 100 + 1}")
            time.sleep(delay)  # Sleep to avoid hitting rate limits
    return results

process_banks(df)

#saving output file
df2=pd.DataFrame()
df2['Regional Office']=df['Regional Office']
df2['NBFC Name']=df['NBFC Name']
df2['Address']=df['Address']
df2['Email ID']=df['Email ID']
df2['Official Website']=df['Official Website']

df2.to_excel('output.xlsx',index=False)


