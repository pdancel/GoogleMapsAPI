
# coding: utf-8

# # Import Libraries 
# <br> These are ultimately other python scripts located in the python directory that you are referencing so that they can do a lot of the heavy lifting tasks. 

# In[11]:


# For Working with DataFrames
import pandas as pd
import numpy as np

# For creating a database connection
import pyodbc

import requests
import datetime
import time

# This section is for creating a log of our geocoding history
import logging
logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# Create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


# # Connect to SQL Server and grab the address data that we need

# In[43]:


# Parameters
server = 'WASPUSMJ040EN0.WKS.JNJ.COM\SPA_MAIN'
db = 'ASP_QA'

# Create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

# query db
sql = """

SELECT [SITE_UCN]
      ,[SITE_ACCOUNT]
      ,[ADDR_ST_NAME]
      ,[CITY]
      ,[STATE]
      ,[ZIP]
      ,ADDR_ST_NAME +', ' + CITY + ', ' + [STATE] + ', ' + [ZIP] + ', ' + 'USA' as [INPUT_ADDRESS]
  FROM [ASP_QA].[dbo].[qryCust_NEED_COORDINATES]
  WHERE ADDR_ST_NAME NOT LIKE 'misc%'




"""
df = pd.read_sql(sql, conn)
df


# In[45]:


print(str(df.shape[0]) + ' rows')


# # Create a list of addresses so that the Google API can iterate over it

# In[46]:


addresses_list = df['INPUT_ADDRESS'].values.tolist()

# Drop duplicate addresses
addresses = list(set(addresses_list))


# In[48]:


print(str(len(addresses)) + ' distinct addresses')


# # Now it's time to set up the environment so Google can do its thang!

# In[49]:


# Set your Google API key here. 
API_KEY = 'AIzaSyChaKZKgV3tYs0T1BQcW_lPS56l77bMUWs'

# Backoff time sets how many minutes to wait between google pings when your API limit is hit
BACKOFF_TIME = 1

# Set your output file name here.
now = datetime.datetime.now()
date_now = now.strftime("%B_%d_%Y")
output_filename = 'tmpData/' + date_now + '.csv' 

# Specify the column name in your input data that contains addresses here
address_column_name = "ADDRESS"

# Return Full Google Results? If True, full JSON results from Google are included in output
RETURN_FULL_RESULTS = False


# # Create a function that will get desired google results

# In[50]:


def get_google_results(address, api_key=None, return_full_response=False):

    # Set up your Geocoding url
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
    if api_key is not None:
        geocode_url = geocode_url + "&key={}".format(api_key)
        
    # Ping google for the reuslts:
    results = requests.get(geocode_url)
    # Results will be in JSON format - convert to dict using requests functionality
    results = results.json()
    
    # if there's no results or an error, return empty results.
    if len(results['results']) == 0:
        output = {
            "formatted_address" : None,
            "latitude": None,
            "longitude": None

        }
    else:    
        answer = results['results'][0]
        output = {
            "formatted_address" : answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng')

        }
        
    # Append some other details:    
    output['input_string'] = address
    output['number_of_results'] = len(results['results'])
    output['status'] = results.get('status')
    if return_full_response is True:
        output['response'] = results
    
    return output


# # Use function to iterate through the address list

# In[51]:


# Create a list to hold results
results = []
# Go through each address in the 'addresses' list
for address in addresses:
    # While the address geocoding is not finished:
    geocoded = False
    while geocoded is not True:
        # Geocode the address with google
        try:
            geocode_result = get_google_results(address, API_KEY, return_full_response=RETURN_FULL_RESULTS)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(address))
            logger.error("Skipping!")
            geocoded = True
            
        # If we're over the API limit, backoff for a while and try again later.
        if geocode_result['status'] == 'OVER_QUERY_LIMIT':
            logger.info("Hit Query Limit! Backing off for a bit.")
            time.sleep(BACKOFF_TIME) # sleep for 1 minute
            geocoded = False
        else:
            # If we're ok with API use, save the results
            # Note that the results might be empty / non-ok - log this
            if geocode_result['status'] != 'OK':
                logger.warning("Error geocoding {}: {}".format(address, geocode_result['status']))
            logger.debug("Geocoded: {}: {}".format(address, geocode_result['status']))
            results.append(geocode_result)           
            geocoded = True

    # Print status every 100 addresses
    if len(results) % 100 == 0:
        logger.info("Completed {} of {} address".format(len(results), len(addresses)))
            
    # Every 500 addresses, save progress to file(in case of a failure so you have something!)
    if len(results) % 500 == 0:
        pd.DataFrame(results).to_csv("{}".format(output_filename))

# All done
logger.info("Finished geocoding all addresses")


# In[52]:


# Convert the list of geocoded results to a DataFrame
results_df = pd.DataFrame(results)
results_df


# In[53]:


results_df.shape


# In[54]:


# # If you want to see the results that failed
# results_df[results_df['status']!='OK']


# In[55]:


# View the columns names
results_df.columns


# In[56]:


# Reorder the Columns to resemble the database
results_df_updated = results_df[['latitude', 'longitude', 'input_string']]
# Rename 'input_string' column to match the DataFrame
results_df_updated.rename(columns={'input_string':'input_address'}, inplace=True)


# In[57]:


results_df_updated.head()


# In[58]:


# Fill any null values with blank spaces
results_df_updated=results_df_updated.fillna('')


# # Now it's time to merge results_df_updated onto the original df

# In[59]:


df_merge = pd.merge(left=df, right=results_df_updated, how = 'left', left_on = ['INPUT_ADDRESS'],right_on = ['input_address'])
df_merge


# In[60]:


df_merge.shape


# # Make the final DataFrame exactly the same as in SQL Server for a clean upload

# In[61]:


# Create and ID Column
df_merge['ID']=''


# In[63]:


df_final = df_merge[['ADDR_ST_NAME', 'SITE_ACCOUNT','latitude','longitude','SITE_UCN','ZIP', 'ID']]


# In[64]:


df_final.rename(columns={'SITE_ACCOUNT':'CUST'}, inplace=True)
df_final.rename(columns={'SITE_UCN':'UCN'}, inplace=True)


# In[65]:


df_final


# # Write results to SQL Server

# In[66]:


import sqlalchemy
from sqlalchemy import create_engine
import urllib

params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=WASPUSMJ040EN0.WKS.JNJ.COM\SPA_MAIN;DATABASE=ASP_QA;Trusted_Connection=yes')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine = create_engine(conn_str)


# In[67]:


# Append DataFrame to the existing table in ASP_QA
df_final.to_sql(con=engine, name='stgCoordinates', if_exists='replace', index=False, chunksize=75)

