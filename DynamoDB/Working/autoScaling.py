import os
import json


file = "528884874493_Config_us-east-1_ConfigHistory_AWS__AutoScaling__AutoScalingGroup_20181011T012430Z_20181011T012430Z_1.json"


with open (file, 'r') as json_file:
    json_obj = json.load(json_file)
    json_str = json_obj['configurationItems']
    for i in json_str:
        print(i['resourceType'])
        break


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