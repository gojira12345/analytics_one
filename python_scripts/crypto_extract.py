import requests
import json
import csv
import configparser
import boto3
from datetime import datetime
import psycopg2


#creating parser object
parser=configparser.ConfigParser()
parser.read('pipeline.conf')

print("Credentials file accessed")



#Redshift Credentials
print("Fetching credentials for Redshift Connection")

dbname=parser.get("aws_creds","database")
user=parser.get("aws_creds","username")
password=parser.get("aws_creds","password")
host=parser.get("aws_creds","host")
port = parser.get("aws_creds", "port")

print("Initiating Redshift connection")


#getting the last_updated from Redshift table

rs_conn=psycopg2.connect(dbname=dbname,user=user,password=password,host=host,port=port)

if rs_conn is None:
   print("Connection to Redshift failed")   
   
else :
    print("Successfully connected to Redshift")
    
    
rs_cursor=rs_conn.cursor()
rs_cursor.execute("SELECT max(last_updated) from public.crypto")    
results=rs_cursor.fetchone()

last_updated_redshift=results[0]
print(f"last_updated_redshift:{last_updated_redshift}")




rs_cursor.close()
rs_conn.close()
    

# Step 1: Access the CoinGecko API for cryptocurrency data
URL = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false'

# Step 2: Make an HTTP GET request to the API
response=requests.get(URL)

if response.status_code==200:
    print("API get request was successful")

else :
    print("not successful")    

print(type(response))
print(type(response.json()))

print(response.json())

all_responses=[]
for  response in response.json():
    cleaned_time=response["last_updated"][:-5]
    cleaned_time=datetime.strptime(cleaned_time,"%Y-%m-%dT%H:%M:%S")
    
    current_response=[]
    
    if last_updated_redshift is None :
        
        
                current_response.append(response["id"])
                current_response.append(response["symbol"])
                current_response.append(response["name"])
                current_response.append(response["image"])
                current_response.append(response["current_price"])
                current_response.append(response["market_cap"])
                current_response.append(response["market_cap_rank"])
                current_response.append(response["fully_diluted_valuation"])
                current_response.append(response["total_volume"])
                current_response.append(response["high_24h"])
                current_response.append(response["low_24h"])
                current_response.append(response["price_change_24h"])
                current_response.append(cleaned_time)
        
    
    elif cleaned_time>last_updated_redshift :
    
                
                current_response.append(response["id"])
                current_response.append(response["symbol"])
                current_response.append(response["name"])
                current_response.append(response["image"])
                current_response.append(response["current_price"])
                current_response.append(response["market_cap"])
                current_response.append(response["market_cap_rank"])
                current_response.append(response["fully_diluted_valuation"])
                current_response.append(response["total_volume"])
                current_response.append(response["high_24h"])
                current_response.append(response["low_24h"])
                current_response.append(response["price_change_24h"])
                current_response.append(cleaned_time)
    
    else : 
          pass
                    
                
    
    all_responses.append(current_response)
    
    
print(all_responses)    

export_file='crypto_export.csv'

with open(export_file ,'w') as fp:
    csv_w=csv.writer(fp,delimiter='|')
    csv_w.writerows(all_responses)
    
fp.close()    

print("Records written to the file")


#uploading thr csv to s3
access_key=parser.get("aws_boto_credentials","access_key")
secret_key=parser.get("aws_boto_credentials","secret_key")
bucket_name=parser.get("aws_boto_credentials","bucket_name")


s3_client=boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
print("Successfully connected to s3 client")

s3_file_name=export_file

s3_client.upload_file(export_file,bucket_name,s3_file_name)
print("File uploaded to  s3")









