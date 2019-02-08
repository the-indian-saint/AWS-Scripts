import boto3
import datetime
from datetime import date, timedelta
import json
import csv
import gzip
import shutil
import os
import pyodbc


server='SD-AE79-EF8E\HCSQLSERVER1,2431'
database='hybrid_cloud'
username = 'testuser'
password ='TestingSQL@1234'
driver = '/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.2.so.0.1'
connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=2431;DATABASE='+database+';UID='+username+';PWD='+password)
cursor = connection.cursor()

cursor.execute("SELECT * from dbo.Inventory_Complete")
rows = cursor.fetchall()
#cursor.execute("SELECT 'Account Name' FROM Aws_Account")
'''
for row in cursor.columns(table='Inventory_Complete'):
    print row.column_name
    #for field in row:
        #print field

'''

profiles = {"528884874493" : 'GRID-PROD',"546156050725" : 'hybadmin-INNOVATION_CATE-LAB-546156050725'}
buckets = {"654430363235": "test-config-bucket-654430363235","528884874493" : 'config-bucket-528884874493',"546156050725":'config-bucket-546156050725'}
prefixes = {"654430363235": "test-config/AWSLogs/654430363235/Config/us-east-1/","528884874493" : "AWSLogs/528884874493/Config/us-east-1/", "546156050725": "splunk.test/AWSLogs/546156050725/Config/us-east-1/"}
years = [2019]
dates = {"2":"5"}#{"1":"31","2":"28","3":"31","4":"30","5":"31","6":"30","7":"31","8":"31","9":"30","10":"31","11":"30","12":"31"}


cores_dict = {"t1.micro" : 1, "t2.nano" :       1, "t2.micro" : 1, "t2.small" : 1, "t2.medium" : 2, "t2.large" : 2, "t2.xlarge" : 4, "t2.2xlarge" : 8, "m1.small" :     1, "m1.medium" : 1, "m1.large" : 2, "m1.xlarge" : 4, "m2.xlarge" : 2, "m2.2xlarge" : 4, "m2.4xlarge" : 8, "m3.medium" : 1, "m3.large" : 2, "m3.xlarge" : 4, "m3.2xlarge" : 8, "m4.large" : 1, "m4.xlarge" :     2, "m4.2xlarge" : 4, "m4.4xlarge" :     8, "m4.10xlarge" : 20, "m4.16xlarge" : 32, "m5.large" : 1, "m5.xlarge" : 2, "m5.2xlarge" : 4, "m5.4xlarge" : 8, "m5.12xlarge" : 24, "m5.24xlarge" :     48, "c1.medium" : 2, "c1.xlarge" : 8, "cc2.8xlarge" : 16, "cg1.4xlarge" : 8, "cr1.8xlarge" : 16, "c3.large" : 1, "c3.xlarge" : 2, "c3.2xlarge" : 4, "c3.4xlarge" : 8, "c3.8xlarge" : 16, "c4.large" : 1, "c4.xlarge" : 2, "c4.2xlarge" : 4, "c4.4xlarge" : 8, "c4.8xlarge" : 18, "c5.large" : 1, "c5.xlarge" : 2, "c5.2xlarge" : 4, "c5.4xlarge" : 8, "c5.9xlarge" : 18, "c5.18xlarge" : 36, "h1.2xlarge" : 4, "h1.4xlarge" : 8, "h1.8xlarge" : 16, "h1.16xlarge" : 32, "hi1.4xlarge" : 8, "hs1.8xlarge" : 8, "g3.4xlarge" : 8, "g3.8xlarge" : 16, "g3.16xlarge" : 32, "g2.2xlarge" : 16, "x1.16xlarge" : 32, "x1.32xlarge" : 64, "x1e.xlarge" : 2, "x1e.2xlarge" : 4, "x1e.4xlarge" : 8, "x1e.8xlarge" : 16, "x1e.16xlarge" : 32, "x1e.32xlarge" : 64, "r4.large" : 1, "r4.xlarge" : 2, "r4.2xlarge" : 4, "r4.4xlarge" : 8, "r4.8xlarge" : 16, "r4.16xlarge" : 32, "r3.large" : 1, "r3.xlarge" : 2, "r3.2xlarge" : 4, "r3.4xlarge" : 8, "r3.8xlarge" : 16, "p2.xlarge" : 2, "p2.8xlarge" : 16, "p2.16xlarge" : 32, "p3.2xlarge" : 4, "p3.8xlarge" : 16, "p3.16xlarge" : 32, "i3.large" : 1, "i3.xlarge" : 2, "i3.2xlarge" : 4, "i3.4xlarge" : 8, "i3.8xlarge" : 16, "i3.16xlarge" : 32, "i2.xlarge" : 2, "i2.2xlarge" : 4, "i2.4xlarge" : 8, "i2.8xlarge" : 16, "d2.xlarge" : 2, "d2.2xlarge" : 4, "d2.4xlarge" : 8, "d2.8xlarge" : 18}


def setProfile():
        for account,profile in profiles.items():
                bucket = buckets[account]
                prefix = prefixes[account]
                print("======================================= INSIDE ACCOUNT ----====================================== ",account)
                print("======================================= USING PROFILE =============================================",profile)
                session = boto3.Session(profile_name=profile)
                cd = session.resource('s3')
                client_cd = session.client('s3')
                ec2_session = boto3.Session(profile_name = profile)
                ec2 = ec2_session.client('ec2','us-east-1')
                main(cd,client_cd,ec2,bucket,prefix,account,profile)

ec2_dict = dict()



def download_VPC(res,cd,bucket,account,profile):

        print("The File Name received is === ", res[res.find('VPC_'):res.find('.g')])
        obj = cd.Object(bucket,res)
        obj.download_file('/home/kl61791/Archive/For_Cores/'+res[res.find('AWS:'):])


        with gzip.open('/home/kl61791/Archive/For_Cores/'+res[res.find('AWS:'):], 'rb') as f_in:
                with open('/home/kl61791/Archive/For_Cores/'+res[res.find('AWS:'):res.find('.g')], 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)


        with open('/home/kl61791/Archive/For_Cores/'+res[res.find('AWS:'):res.find('.g')], 'r') as filename:
                my_dict = json.load(filename)


        json_length = len(my_dict['configurationItems'])
        print(json_length)
        networkInterfaceType=""
        test = dict()
        j=0
        while j < len(my_dict['configurationItems']):
                print("=======================================")
                print("=======================================")
                print("=============", j,"==========================")
                i = 0
                while i <len(my_dict['configurationItems'][j]['relationships']):
                        #print(my_dict['configurationItems'][j]['relationships'][i]['resourceType'])
                        resourceType = my_dict['configurationItems'][j]['relationships'][i]['resourceType']
                        #print(my_dict['configurationItems'][j]['relationships'][i]['resourceId'])
                        resourceId = my_dict['configurationItems'][j]['relationships'][i]['resourceId']

                        #print(my_dict['configurationItems'][j]['tags'])
                        try:

                                tags = my_dict['configurationItems'][j]['tags']['Name']
                        except:
                                tags = ""
                        #print(my_dict['configurationItems'][j]['configurationItemCaptureTime'])
                        captureTime = my_dict['configurationItems'][j]['configurationItemCaptureTime']

                        ownerId = str(account)
                        try:

                                vpcId = my_dict['configurationItems'][j]['configuration']['vpcId']
                        except:
                                vpcId =""
                        test[resourceId,resourceType,captureTime,vpcId,ownerId,tags] = test.get(resourceId,0)+1
                        i+=1
                j+=1
        #print(ownerId)
        #print(vpcId)
        for key,values in test.items():
                try:
                        cursor.execute("insert into Inventory_awss4(Account, resourceType,resourceID,captureTime,Tags) values (?,?,?,?,?)",key[4],key[1],key[0],key[2],key[5])
                        connection.commit()
                except:
                        continue
        #i=0
        #while i < json_length :
                #cidr = my_dict['configurationItems'][i]['configuration']['cidrBlock']
                #vpcId = my_dict['configurationItems'][i]['configuration']['vpcId']
                #ownerId = my_dict['configurationItems'][i]['configuration']['ownerId']
                #print(cidr)
                #print(vpcId)
                #print(ownerId)
                #i +=1
def main(cd,client_cd,ec2,bucket,prefix,account,profile):

        try:
                os.remove('/home/kl61791/Archive/For_Cores/ec2_cores_'+bucket+'.csv')
                #os.remove('/home/kl61791/Archive/actual_ec2_change.csv')
                for filename in os.listdir('/home/kl61791/Archive/For_Cores/'):
                        if filename.endswith('.json') or filename.endswith('.json.gz'):
                                os.unlink(filename)
                                #print(filename)
                print("Files Removed")

        except:
                print("File does not exist")

        total_running_instances = {}

        now=datetime.datetime.utcnow().strftime('%Y-%m-%d')
        tomorrow=(date.today()+timedelta(1)).strftime('%Y-%m-%d')
        yesterday=(datetime.datetime.utcnow()-timedelta(1)).strftime('%Y-%m-%d')
        print("Today is",now)
        #print("Splitted Date is",now.split('-'))

        joined_date = ''.join(yesterday.split('-'))
        #print(joined_date)

        print("yesterday was ",yesterday)
        #Year= yesterday.split('-')[0]
        #Month= int(yesterday.split('-')[1])
        #Day =int(yesterday.split('-')[2])
        #print(Year,Month,"DAY IS ",Day)
        print("BUCKET NAME IS ==================================== ",bucket)
        print("PREFIX IS ========================================== ",prefix)
        #try:
        for Year in years:


                for Month,days in dates.items():
                        print("This is month ",str(Month))
                        #print(days)
                        Day=1
                        while Day<= int(days):
                                print(Day,Month,Year)
                                Prefix =prefix+str(Year)+'/'+str(Month)+'/'+str(Day)+'/ConfigHistory/'
                                print("PREFIX I S   ",Prefix)
                                response = client_cd.list_objects(Bucket = bucket, Prefix =prefix+str(Year)+'/'+str(Month)+'/'+str(Day)+'/ConfigHistory/')
                                try:

                                        for res in response['Contents']:
                                                #print(res)
                                                if res['Key'].find('AWS:') != -1:
                                                        #print(res['Key'])
                                                        download_VPC(res['Key'],cd,bucket,account,profile)
                                except:
                                        Day +=1
                                        print("INSIDE EXCEPT ------- ",Day)
                                        continue
                                Day+=1

#main()
setProfile()

