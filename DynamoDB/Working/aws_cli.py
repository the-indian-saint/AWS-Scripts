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
                            if i['resourceCreationTime']:
                                new_dict['resourceCreationTime'] = i['resourceCreationTime']
                            elif i['instanceCreateTime']:
                                new_dict['resourceCreationTime'] = i['instanceCreateTime']
                            else:
                                new_dict['resourceCreationTime'] = "Not Mentioned"
                        except Exception as e:
                            print(e)
                        try:
                            cursor.execute("insert into Inventory_awss3(resourceId, awsAccountId, configurationItemStatus, resourceType, arn, awsRegion,configurationStateId,resourceCreationTime) values (?,?,?,?,?,?,?,?)",str(new_dict['resourceId']),str(new_dict['awsAccountId']),str(new_dict['configurationItemStatus']),str(new_dict['resourceType']),str(new_dict['arn']),str(new_dict['awsRegion']), str(new_dict['configurationStateId']), str(new_dict['resourceCreationTime']))
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)
            os.remove(file)

main()
connection.commit()
connection.close()


c