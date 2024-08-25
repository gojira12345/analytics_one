import boto3
import configparser
import pymysql
import psycopg2
import csv


#creating a parser object ,that will be later used to fetch credentials and store it in variables
parser=configparser.ConfigParser()
parser.read('pipeline.conf')

#s3 credentials





#storing the S3 client connection credentials
access_key=parser.get("aws_boto_credentials","access_key")
secret_key=parser.get("aws_boto_credentials","secret_key")
bucket_name=parser.get("aws_boto_credentials","bucket_name")

#Storing Redshift connection credentials
dbname=parser.get("aws_creds","database")
host=parser.get("aws_creds","host")
user=parser.get("aws_creds","username")
password=parser.get("aws_creds","password")
port=parser.get("aws_creds","port")



#initiate a connection request to redshift
rs_conn=psycopg2.connect(dbname=dbname,user=user,password=password,host=host,port=port)

if rs_conn is None:
    print("Unable to connect to Redshift")
else:
    print("Successfully connect to Redshift")    
    
    
#need to fetch the max(last updated timestamp ) from Orders table in Redshift

query_max_last_updated_date=f"SELECT COALESCE(MAX(lastupdated),'1900-01-01') FROM public.Orders"    


#creating a cusrsor obeject that will be used to execute the sql query and fetch the result
rs_cursor=rs_conn.cursor()
rs_cursor.execute(query_max_last_updated_date)
results=rs_cursor.fetchone()

#got the last_updated_time now in incremental extraction only records post this will be fetched
last_updated_warehouse=results[0]

print(last_updated_warehouse)

rs_conn.commit()
rs_cursor.close()



##storing the credentials for connection to Mysql RDS db
hostname=parser.get("mysql_config","hostname")
port=parser.get("mysql_config","port")
username=parser.get("mysql_config","username")
dbname=parser.get("mysql_config","database")
password= parser.get("mysql_config","password")

#Creating connection to Mysql DB
conn=pymysql.connect(host=hostname,user=username,password=password,db=dbname,port=int(port))

if conn is None:
    print("Unable to connect to Mysql Database")
else:
    print("Successfully connected to Mysql Database")    
    

#Creating the Query for extracting Incremental data
my_sql_query="SELECT * FROM Orders WHERE LastUpdated > %s"

mysql_cursor=conn.cursor()
mysql_cursor.execute(my_sql_query,(last_updated_warehouse))       

results=mysql_cursor.fetchall()  
print(results)  


#creating a name for the file where extracted data will be stored
local_filename="orders_extract.csv"

with open(local_filename ,"w") as fp:
    csv_w=csv.writer(fp,delimiter='|')
    csv_w.writerows(results)
    
print("Records written to the file")

fp.close()    
conn.close()
mysql_cursor.close()
#now need to push to data to s3  

# load the aws_boto_credentials values
access_key=parser.get("aws_boto_credentials","access_key")
secret_key=parser.get("aws_boto_credentials","secret_key")
bucket_name=parser.get("aws_boto_credentials","bucket_name")

#setting up s3 client
s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

s3_file = local_filename

s3.upload_file(local_filename, bucket_name, s3_file)

print("File successfuly uploaded to S3")         
                     

    
    