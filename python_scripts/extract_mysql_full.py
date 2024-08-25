import pymysql
import csv
import boto3
import configparser 
import psycopg2



#cresting a parser object that will parse the credentials required from pipeline.conf file

parser=configparser.ConfigParser()
parser.read("pipeline.conf")


#getting the credentials for Mysql database connection and setting up the connection

hostname=parser.get("mysql_config","hostname")
port =parser.get("mysql_config","port")
username=parser.get("mysql_config","username")
dbname=parser.get("mysql_config","database")
password=parser.get("mysql_config","password")


conn=pymysql.connect(host=hostname,user=username,password=password,db=dbname,port=int(port))

if conn is None:
    print("Error in connecting to Mysql database")
else :
    print("Successfully connected to Mysql Database")    


#Query
m_query="SELECT * FROM Orders;"

#creating a name for the file where extracted data will be stored
local_filename="orders_extract.csv"

#creating a cusrson object ,executing the query and fetching the rows
m_cursor=conn.cursor()
m_cursor.execute(m_query)
results=m_cursor.fetchall()

print(results)

#creating a file pointers
with open(local_filename,"w") as fp:
    csv_w=csv.writer(fp,delimiter='|')
    csv_w.writerows(results)

print("Records written to the file")

fp.close()
conn.close()
m_cursor.close()


 # load the aws_boto_credentials values
access_key=parser.get("aws_boto_credentials","access_key")
secret_key=parser.get("aws_boto_credentials","secret_key")
bucket_name=parser.get("aws_boto_credentials","bucket_name")

#setting up s3 client
s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

s3_file = local_filename

s3.upload_file(local_filename, bucket_name, s3_file)

print("File successfuly uploaded to S3")



