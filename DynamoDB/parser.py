import json
from pprint import pprint
import boto3
import pyodbc
import os
import gzip
import shutil

s3 = boto3.client('s3')
server='SD-AE79-EF8E\HCSQLSERVER1,2431'
database='hybrid_cloud'
username = 'testuser'
password ='TestingSQL@1234'
driver = '/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.2.so.0.1'
bucket = "parser-demo"
connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=2431;DATABASE='+database+';UID='+username+';PWD='+password)
cursor = connection.cursor()

def lambda_handler():
    object_list = s3.list_objects(Bucket=bucket)
    for objects in object_list['Contents']:
        json_obj = objects['Key']
        target = json_obj.split('/')
        target_file = '-'.join(target)
        #length = len(target) - 1
        #target_obj = target[length]
        with open (target_file, 'wb') as data:
            s3.download_fileobj(bucket, json_obj, data)
    for f in os.listdir('.'):
        if f.endswith('.gz'):
            with gzip.open(f, 'rb') as f_in:
                file_name_json = f[0:-3]
                file_name = str(file_name_json) + ".json" 
                with open(file_name, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            os.remove(f)
    for f in os.listdir('.'):
        with open (f, 'r') as json_file:
            if f.endswith('.json'):
                try:
                    json_obj = json.load(json_file)
                    json_str = json_obj['configurationItems']
                    new_dict = {}
                    for i in json_str:
                        new_dict['resourceType'] = i['resourceType']
                        new_dict['resourceId'] = i['resourceId']
                        try:
                            new_dict['arn'] = i['ARN']
                        except Exception:
                            new_dict['arn'] = None
                            error = "No ARN in file: %s" %(f)
                            print(error)
                        new_dict['relatedEvents'] = i['relatedEvents'] if i['relatedEvents'] else None
                        new_dict['relationships'] = i['relationships'] if i['relationships'] else None
                        new_dict['supplementaryConfiguration'] = i['supplementaryConfiguration'] if i['supplementaryConfiguration'] else None
                        new_dict['tags'] = i['tags'] if i['tags'] else None
                        new_dict['configurationItemVersion'] = i['configurationItemVersion'] if i['configurationItemVersion'] else None
                        new_dict['configurationItemCaptureTime'] = i['configurationItemCaptureTime']
                        new_dict['awsAccountId'] = i['awsAccountId']
                        new_dict['configurationItemStatus'] = i['configurationItemStatus']
                        new_dict['awsRegion'] = i['awsRegion']
                        new_dict['configurationStateMd5Hash'] = i['configurationStateMd5Hash'] if i['configurationStateMd5Hash'] else None
                        #print(new_dict)
                        cursor.execute("INSERT INTO Inventory_Complete(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?)" , new_dict['resourceId'],
                        new_dict['relatedEvents'], new_dict['relationships'], new_dict['supplementaryConfiguration'], new_dict['tags'], new_dict['configurationItemVersion'],
                        new_dict['awsAccountId'], new_dict['configurationItemStatus'], new_dict['configurationStateMd5Hash'], new_dict['resourceType'], new_dict['arn'],
                        new_dict['awsRegion'])         
                        #cursor.execute(command)              
                        #connection.commit()
                        #connection.close()
                   
                except Exception as e:
                    error = "Error on" + str(f)
                    print(error)


lambda_handler()