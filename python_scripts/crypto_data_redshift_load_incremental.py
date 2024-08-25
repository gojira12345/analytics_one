import configparser
import psycopg2


parser=configparser.ConfigParser()
parser.read('pipeline.conf')
print("Credentials file fetched ")

#Redshift Credentials
print("Fetching credentials for Redshift Connection")

dbname=parser.get("aws_creds","database")
user=parser.get("aws_creds","username")
password=parser.get("aws_creds","password")
host=parser.get("aws_creds","host")
port = parser.get("aws_creds", "port")

print("Initiating Redshift connection")
rs_conn=psycopg2.connect(dbname=dbname,user=user,password=password,host=host,port=port)

if rs_conn is None:
   print("Connection to Redshift failed")   
   
else :
    print("Successfully connected to Redshift")
    
  
#getting account id and iam role and bucket  details from which redshift will  copy data
account_id=parser.get("aws_boto_credentials","account_id")
iam_role=parser.get("aws_creds","iam_role")   
bucket_name=parser.get("aws_boto_credentials","bucket_name")  

#s3://pipeline-bucket2/orders_extract.csv
file_path_str="s3://" + bucket_name  + "/crypto_export.csv"
#arn:aws:iam::010928204820:role/RedshiftLoadRole
iam_role_str="arn:aws:iam::"+account_id+":role/"+iam_role

#Copying the data from s3 to Redshift table
rs_query="COPY public.crypto" + " FROM %s" + " iam_role %s"

rs_cursor=rs_conn.cursor()
rs_cursor.execute(rs_query,(file_path_str,iam_role_str))

rs_cursor.close

rs_conn.commit()
print("Commiting the updation of records in Redshift")

rs_conn.close()
print("Records successfully ingested into Redshift Table")


