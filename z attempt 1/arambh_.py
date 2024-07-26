import pandas as pd
import numpy as np

df=pd.read_excel("NBFCsandARCs10012023 (5).XLSX")

df.describe().loc['count']


import serpapi

def result(q,location):
  client=serpapi.Client(api_key='d95eb720403196351b6050acd7b7d6688c1a71209a7a7b9809b29ffe239fe554')
  result=client.search(
    q=q,
    engine="google",
    location=location,
    hl="en",
    gl="in",
    num="1"
 )
  for item in result['organic_results']:
    return (item['link'])

df['Official Website']=np.nan

limit=False
for index,row in df.iterrows():
  if limit==True:
    break

  if(pd.isna(df['NBFC Name'][index])==False):
    try:
      df['Official Website'][index]=result(df['NBFC Name'][index],df['Regional Office'][index])
      print("index ",index," found\n")

    except Exception as e:
      df['Official Website'][index]="data not available"
      if "429" in str(e):
        reason="search limit reached"
        limit=True
      if "500" in str(e):
        reason="Internal Server error"
      if "401" in str(e):
        reason="Unauthorised"
      if "402" in str(e):
        reason="Payment Required"
      if "404" in str(e):
        reason="Not Found"

      print("index ",index," not found due to ", reason)
  else:
    print("index ",index," not found due to missing data")


#saving output file
df2=pd.DataFrame()
df2['Regional Office']=df['Regional Office']
df2['NBFC Name']=df['NBFC Name']
df2['Address']=df['Address']
df2['Email ID']=df['Email ID']
df2['Official Website']=df['Official Website']

df2.to_excel('output.xlsx',index=False)
