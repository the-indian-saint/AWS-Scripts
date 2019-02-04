import json
from pprint import pprint
import boto3
import pyodbc
import os
import gzip
import shutil

#ddb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
server='SD-AE79-EF8E\HCSQLSERVER1,2431'
database='hybrid_cloud'
username = 'testuser'
password ='TestingSQL@1234'
driver = '/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.2.so.0.1'

connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=2431;DATABASE='+database+';UID='+username+';PWD='+password)
cursor = connection.cursor()
bucket = "config-bucket-528884874493"
def lambda_handler():
    object_list = s3.list_objects(Bucket=bucket)
    for objects in object_list['Contents']:
        obj_json = objects['Key']
        target = obj_json.split('/')
        length = len(target) - 1
        target_obj = target[length]
        with open (target_obj, 'wb') as data:
            s3.download_fileobj(bucket, obj_json, data)
        if target_obj.endswith('.gz'):
            with gzip.open(target_obj, 'rb') as f_in:
                file_name_json = target_obj[0:-3]
                with open(file_name_json, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(target_obj)
        else:
            os.remove(target_obj)
        for f in os.listdir('.'):
            if f.endswith('.json'):
                with open (f, 'r') as json_file:
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
                            new_dict['relatedEvents'] = i['relatedEvents'] if i['relatedEvents'] else None
                            new_dict['relationships'] = i['relationships'] if i['relationships'] else None
                            new_dict['configurationStateId'] = i['configurationStateId'] if i['configurationStateId'] else None
                            new_dict['tags'] = i['tags'] if i['tags'] else None
                            new_dict['configurationItemVersion'] = i['configurationItemVersion'] if i['configurationItemVersion'] else None
                            new_dict['configurationItemCaptureTime'] = i['configurationItemCaptureTime']
                            new_dict['awsAccountId'] = i['awsAccountId']
                            new_dict['configurationItemStatus'] = i['configurationItemStatus']
                            new_dict['awsRegion'] = i['awsRegion']
                            new_dict['configurationItemCaptureTime'] = i['configurationItemCaptureTime'] if i['configurationItemCaptureTime'] else None
                            try:
                                cursor.execute("insert into Inventory_awss3(resourceId, awsAccountId, configurationItemStatus, resourceType, arn, awsRegion,configurationItemCaptureTime,configurationStateId) values (?,?,?,?,?,?,?,?)",str(new_dict['resourceId']),str(new_dict['awsAccountId']),str(new_dict['configurationItemStatus']),str(new_dict['resourceType']),str(new_dict['arn']),str(new_dict['awsRegion']), str(new_dict['configurationItemCaptureTime']), str(new_dict['configurationStateId']))
                            except Exception as e:
                                print(e)
                    except Exception as e:
                        print(e)
                os.remove(f)        
#lambda_handler()
#connection.commit()
#connection.close()