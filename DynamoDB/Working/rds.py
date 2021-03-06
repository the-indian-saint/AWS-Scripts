import json
from pprint import pprint
import boto3
import pyodbc
import os
import gzip
import shutil
import subprocess as sp


server='SD-AE79-EF8E\HCSQLSERVER1,2431'
database='hybrid_cloud'
username = 'testuser'
password ='TestingSQL@1234'
driver = '/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.2.so.0.1'
connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=2431;DATABASE='+database+';UID='+username+';PWD='+password)
cursor = connection.cursor()
bucket = "config-bucket-528884874493"
work_dir = ""


def download_objects(bucket, work_dir):
    command = "aws s3 cp --recursive s3://%s %s" %(bucket, work_dir)
    sp.Popen(command).wait()
    print('All Objects Downloaded')

def get_all_file_paths(directory): 
  
    # initializing empty file paths list 
    file_paths = [] 
  
    # crawling through directory and subdirectories 
    for root, directories, files in os.walk(directory): 
        for filename in files: 
            # join the two strings in order to form the full filepath. 
            filepath = os.path.join(root, filename) 
            file_paths.append(filepath) 
  
    # returning all file paths 
    return file_paths

def unzip_files(file_paths):
    for files in file_paths:
        if files.endswith('.gz'):
            with gzip.open(files, 'rb') as f_in:
                file_name_json = files[0:-3]
                with open(file_name_json, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                os.remove(files)

def main():
    download_objects(bucket, work_dir)
    file_paths = get_all_file_paths('.')
    unzip_files(file_paths)
    file_paths2 = get_all_file_paths('.')
    for file in file_paths2:
        if file.endswith('.json'):
            with open (file, 'r') as json_file:
                try:
                    json_obj = json.load(json_file)
                    json_str = json_obj['configurationItems']
                    new_dict = {}
                    for i in json_str:
                        new_dict['dBInstanceIdentifier'] = i['configuration']['dBInstanceIdentifier']
                        new_dict['dBInstanceClass'] = i['configuration']['dBInstanceClass']
                        new_dict['dBInstanceStatus'] = i['configuration']['dBInstanceStatus']
                        new_dict['dbiResourceId'] = i['configuration']['dbiResourceId']
                        new_dict['configurationItemCaptureTime'] = i['configurationItemCaptureTime']
                        new_dict['dBInstanceArn'] = i['configuration']['dBInstanceArn']
                        new_dict['instanceCreateTime'] = i['configuration']['instanceCreateTime']
                        new_dict['resourceId'] = i['resourceId']
                        new_dict['awsAccountId'] = i['awsAccountId']
                        try:
                            cursor.execute("insert into Inventory_awsrds(resourceId, dBInstanceClass, dBInstanceStatus, configurationItemCaptureTime, dBInstanceArn, dBInstanceIdentifier) values (?,?,?,?,?,?)",str(new_dict['resourceId']),str(new_dict['dBInstanceClass']),str(new_dict['dBInstanceStatus']),str(new_dict['configurationItemCaptureTime']),str(new_dict['dBInstanceArn']),str(new_dict['dBInstanceIdentifier']))
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)
            os.remove(file)

main()
connection.commit()
connection.close()
#file = "546156050725_Config_us-east-1_ConfigHistory_AWS__RDS__DBInstance_20190122T113622Z_20190122T113622Z_1.json"
#with open (file, 'r') as json_file:
    #json_obj = json.load(json_file)
    #json_str = json_obj['configurationItems']
    #for i in json_str:
        #print(i['configurationItemCaptureTime'])
        #break

