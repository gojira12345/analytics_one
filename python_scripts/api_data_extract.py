import requests
import json
import configparser
import csv
import boto3


base_url = f"https://api.wheretheiss.at/v1/satellites/25544/positions"
params = {
        'timestamps':1436029892,
        'units': 'miles'
    }
response = requests.get(base_url, params=params)

print(response.content)

response_json=json.loads(response.content)









print(type(response_json))

final=[]
for i in response_json:
   current=[] 
   current.append(i['name'])
   current.append(i['id'])
   current.append(i['latitude'])
   current.append(i['longitude'])
   current.append(i['altitude'])
   current.append(i['visibility'])
   
final.append(current)   
print(final)

export_file='export.csv'

with open(export_file ,'w') as fp:
    csv_writer=csv.writer(fp,delimiter='|')
    csv_writer.writerows(final)
    
fp.close()    

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials","access_key")
secret_key = parser.get("aws_boto_credentials","secret_key")
bucket_name = parser.get("aws_boto_credentials","bucket_name")
s3 = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
s3.upload_file(export_file,bucket_name,export_file)