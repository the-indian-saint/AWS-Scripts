import boto3
import datetime
from datetime import date, timedelta
import json
import csv
import gzip
import shutil
import os
import pyodbc

#server='SD-AE79-EF8E\HCSQLSERVER1,2431'
#database='hybrid_cloud'
#username = 'testuser'
#password ='TestingSQL@1234'
#driver = '/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.2.so.0.1'
#connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=2431;DATABASE='+database+';UID='+username+';PWD='+password)
#cursor = connection.cursor()

#cursor.execute("SELECT * from dbo.Inventory_Complete")
#rows = cursor.fetchall()
#cursor.execute("SELECT 'Account Name' FROM Aws_Account")
'''
for row in cursor.columns(table='Inventory_Complete'):
    print row.column_name
    #for field in row:
        #print field

'''

profiles = {"528884874493" : 'GRID-PROD'}
buckets = {"654430363235": "test-config-bucket-654430363235","528884874493" : 'config-bucket-528884874493'}
prefixes = {"654430363235": "test-config/AWSLogs/654430363235/Config/us-east-1/","528884874493" : "AWSLogs/528884874493/Config/us-east-1/"}

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
                main(cd,client_cd,ec2,bucket,prefix)

ec2_dict = dict()
def ec2_details(ec2,bucket):

        #ec2_session = boto3.Session(profile_name = 'GRID-PROD')
        #ec2 = ec2_session.client('ec2','us-east-1')

        ec2_instances = ec2.describe_instances()
        #print(ec2_instances)
        
        count = 0
        length = 0
        instance_type = dict()
        fieldnames = ['ResourceID', 'ResourceState', 'No_Of_Cores', 'ResourceCapture', 'ResourceTerminateTime','InstanceType']
        for reservation in ec2_instances['Reservations']:
                #print(reservation['Instances'][0]['InstanceType'])
                #count = count + cores_dict[reservation['Instances'][0]['InstanceType']]
                for ins_type in reservation['Instances']:
                        print("ins_type['State']['Name'] ============================================================================================================================",ins_type['State']['Name'])
                        length +=1
                        #print(ins_type)
                        count = count + cores_dict[ins_type['InstanceType']]
                        #print(ins_type['InstanceType'])
                        #print(ins_type['InstanceId'])
                        #print(cores_dict[ins_type['InstanceType']])
                        #print(ins_type['LaunchTime'])
                        instance_type[ins_type['InstanceType']] = instance_type.get(ins_type['InstanceType'],0) + 1
                        ec2_dict[ins_type['InstanceId']] = ec2_dict.get(ins_type['InstanceId'],0)+1
                        #cursor.execute("insert into Inventory_Complete(ResourceID, ResourceState, No_Of_Cores, ResourceCapture,ResourceTerminateTime,InstanceType) values (?,?,?,?,?,?)",ins_type['InstanceId'],ins_type['State']['Name'],str(cores_dict[ins_type['InstanceType']]),ins_type['LaunchTime'],'',ins_type['InstanceType'])
                        #connection.commit()
                        

                                
        #print(len(ec2_instances['Reservations']))
        print("Total No Of Instances are ------------------------ ",length)
        print("Total core count ", count)
        for keys,values in instance_type.items():
                #print(keys)
                vpcu_by_instance_type = cores_dict[keys]
                cost = vpcu_by_instance_type * values
                #print(cost)
                instance_type[keys] = cost
        #print(instance_type)
        #print(ec2_dict)
        print(len(ec2_dict))
        return(count, length, instance_type)

def download_files(res,total_running_instances,cd,bucket):

        print("The File Name received is === ", res[res.find('Instance_'):res.find('.g')])
        obj = cd.Object(bucket,res)
        obj.download_file('/home/kl61791/Archive/For_Cores/'+res[res.find('Instance_'):])

        with gzip.open('/home/kl61791/Archive/For_Cores/'+res[res.find('Instance_'):], 'rb') as f_in:
                print(res[res.find('Instance_'):])
                with open('/home/kl61791/Archive/For_Cores/'+res[res.find('Instance_'):res.find('.g')], 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                        try:
                                print(f_in, f_out)
                        except Exception:
                                pass

        with open('/home/kl61791/Archive/For_Cores/'+res[res.find('Instance_'):res.find('.g')], 'r') as filename:
                my_dict = json.load(filename)

        json_length = len(my_dict['configurationItems'])
        print(json_length)
        i=0
        while i < json_length :
                with open('/home/kl61791/Archive/For_Cores/ec2_cores_'+bucket+'.csv', 'a') as csvfile:
                        fileEmpty = os.stat('/home/kl61791/Archive/For_Cores/ec2_cores_'+bucket+'.csv').st_size == 0
                        fieldnames = ['ResourceID', 'ResourceState', 'No_Of_Cores', 'ResourceCapture', 'ResourceTerminateTime','InstanceType']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if fileEmpty :
                                writer.writeheader()
                        if my_dict['configurationItems'][i]['configurationItemStatus'] == "ResourceDiscovered":

                                print(my_dict['configurationItems'][i]['configurationItemStatus'])
                                corecount = my_dict['configurationItems'][i]['configuration']['cpuOptions']['coreCount']
                                threadpercore = my_dict['configurationItems'][i]['configuration']['cpuOptions']['threadsPerCore']
                                vCPU=corecount*threadpercore
                                print("No Of Cores ",corecount)
                                print(my_dict['configurationItems'][i]['resourceId'])
                                print(my_dict['configurationItems'][i]['configuration']['instanceId'])
                                print(my_dict['configurationItems'][i]['configuration']['instanceType'])
                                print(my_dict['configurationItems'][i]['configuration']['launchTime'])
                                launchtime = my_dict['configurationItems'][i]['configuration']['launchTime']
                                total_running_instances[my_dict['configurationItems'][i]['resourceId']] = total_running_instances.get(my_dict['configurationItems'][i]['resourceId'],0) + 1 
                                print("====================================== ")
                                #cursor.execute("insert into Inventory_Complete(ResourceID, ResourceState, No_Of_Cores, ResourceCapture,ResourceTerminateTime,InstanceType) values (?,?,?,?,?,?)",my_dict['configurationItems'][i]['resourceId'],my_dict['configurationItems'][i]['configurationItemStatus'],str(corecount),launchtime,'',my_dict['configurationItems'][i]['configuration']['instanceType'])
                                #connection.commit()

                                writer.writerow({
                                        'ResourceID': my_dict['configurationItems'][i]['resourceId'],
                                        'ResourceState': my_dict['configurationItems'][i]['configurationItemStatus'],
                                        'No_Of_Cores': corecount,
                                        'ResourceCapture': launchtime,
                                        'ResourceTerminateTime': "",
                                        'InstanceType': my_dict['configurationItems'][i]['configuration']['instanceType']
                                        })
                        elif my_dict['configurationItems'][i]['configurationItemStatus'] == "ResourceDeleted":
                                print(my_dict['configurationItems'][i]['configurationItemStatus'])
                                print(my_dict['configurationItems'][i]['resourceId'])
                                print(my_dict['configurationItems'][i]['configurationItemCaptureTime'])
                                #print(my_dict['configurationItems'][i]['configuration']['instanceType'])
                                corecount = 0#my_dict['configurationItems'][i]['configuration']['cpuOptions']['coreCount']
                                threadpercore = 0#my_dict['configurationItems'][i]['configuration']['cpuOptions']['threadsPerCore']
                                launchtime = ''
                                total_running_instances[my_dict['configurationItems'][i]['resourceId']] = total_running_instances.get(my_dict['configurationItems'][i]['resourceId'],0) + 1
                                print("===================================== ")
                                #cursor.execute("insert into Inventory_Complete(ResourceID, ResourceState, No_Of_Cores, ResourceCapture,ResourceTerminateTime,InstanceType) values (?,?,?,?,?,?)",my_dict['configurationItems'][i]['resourceId'],my_dict['configurationItems'][i]['configurationItemStatus'],str(corecount),launchtime,my_dict['configurationItems'][i]['configurationItemCaptureTime'],'')
                                #connection.commit()             
                                writer.writerow({
                                        'ResourceID': my_dict['configurationItems'][i]['resourceId'],
                                        'ResourceState': my_dict['configurationItems'][i]['configurationItemStatus'],
                                        'No_Of_Cores': corecount,
                                        'ResourceCapture': launchtime,
                                        'ResourceTerminateTime': my_dict['configurationItems'][i]['configurationItemCaptureTime'],
                                        'InstanceType':"" 
                                        })

                i +=1

               
#ec2_details()

#print(total_running_instances)

def main(cd,client_cd,ec2,bucket,prefix):

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
        Year= yesterday.split('-')[0]
        Month= int(yesterday.split('-')[1])
        Day =int(yesterday.split('-')[2])

        print("BUCKET NAME IS ==================================== ",bucket)
        print("PREFIX IS ========================================== ",prefix)
        try:

                response = client_cd.list_objects_v2(Bucket = bucket, Prefix =prefix+Year+'/'+str(Month)+'/'+str(Day)+'/ConfigHistory/')

                for res in response['Contents']:
                        #print(res)
                        if res['Key'].find('AWS::EC2::Instance') != -1:
                                print(res['Key'])
                                download_files(res['Key'],total_running_instances,cd,bucket)
                                #print(total_ins)
                ec2_details(ec2,bucket) 
                #connection.commit()
        except:
                print("THERE ARE NO EC2 INSTANCES RECORDED IN AWS CONFIG AS OF NOW, RUN AGAIN AFTER SOME TIME")
        #ec2_details(ec2,bucket)
        #connection.commit()

#main()
setProfile()
