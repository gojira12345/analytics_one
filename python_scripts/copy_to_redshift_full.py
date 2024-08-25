import pymysql
import csv
import boto3
import configparser 
import psycopg2

parser=configparser.ConfigParser()

parser.read("pipeline.conf")

dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")  # This should be your Redshift Serverless endpoint
port = parser.get("aws_creds", "port")

rs_conn =psycopg2.connect(dbname=dbname,user=user,password=password,host=host,port=port)

if rs_conn is None:
    print("no")
    
else:
    print("Connection to Redshift succcesful")    
    
    
 # load the account_id and iam_role from the
 # conf files
account_id=parser.get("aws_boto_credentials","account_id")
iam_role=parser.get("aws_creds","iam_role")
bucket_name = parser.get("aws_boto_credentials","bucket_name")    

file_path = ("s3://"+ bucket_name+ "/export.csv")
role_string = ("arn:aws:iam::"+ account_id+ ":role/" + iam_role)

#s3://pipeline-bucket2/orders_extract.csv
#arn:aws:iam::010928204820:role/RedshiftLoadRole


#truncate the Redshift table 
trunc_sql="TRUNCATE public.sats"

#creating the sql COPY comand using the string created with placeholders for them
sql="COPY public.sats" +" FROM %s" +" iam_role %s"

cursor=rs_conn.cursor()

cursor.execute(trunc_sql)
print("Redshift Orders Table truncated")

cursor.execute(sql,(file_path,role_string))

    
cursor.close()

#need to commit the changes so that database records are commited
rs_conn.commit() 
 # close the connection
rs_conn.close()    
