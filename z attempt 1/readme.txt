requirements- 
pip install serpapi
pip install pandas
pip install numpy

working for users:

if you download my files from GitHub as it is and put them in a folder , you just need to open the folder in vs studio and run it nothing else to do.
 incase your excel file is stored else where you need to change the path in line 4
if my api key at line 12 doesn't work replace it with your serp api key.

technical working:
this program would automatically read the provided excel file and create a new data frame df2 consisting of fields : Regional Office, NBFC Name, Address,Email ID, Official Website.

while in the process of scraping official websites it woud tell the user of its progress about what index bank has been found which has not been found and also tell the error faced if any like search limit reached 
or missing data or unauthorised keyword.

and no need to worry serpapi is a legal api
 
it would also ask to save the output excel file , incase you don't want to download it and just see the results edit the codes last 3 lines and replace them with df2.head(), this would show the top few rows of the new df formed..

ARAMBH_.pyntb is the practice py notebook i worked on
arambh.py is your program
the excel sheet i worked on is provided 
output file which came out in my system took 50 s to reach index 34 and marked it as search limit reached so it didn't went further

stay happy and stay blessed :)