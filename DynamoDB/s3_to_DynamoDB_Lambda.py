import json
from pprint import pprint
import boto3

ddb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    #as this is a lambda function triggerd by a 'Create' event, we can directly get the bucket name and object name using the 'event'. To understand more
    # try printing the json object 'event' in the lambda function & see the output in CloudWatch logs. CloudWatch logs permissions required for the role. 
    bucket = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    json_object = s3.get_object(Bucket=bucket, Key=file_name)
    jsonFileReader = json_object['Body'].read()
    jsondict = json.loads(jsonFileReader)
    #with open ('Instance_20190116T000259Z_20190116T003908Z_1.json', 'r') as json_file:
    #json_obj = json.load(json_file)
    json_str = jsondict['configurationItems']
    new_dict = {}
    for i in json_str:
        table = ddb.Table('test_db')
        new_dict['resourceType'] = i['resourceType']
        new_dict['resourceId'] = i['resourceId']
        try:
                new_dict['arn'] = i['ARN']
        except Exception as e:
                print(e)
                new_dict['arn'] = None
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
        table.put_item(Item=new_dict)
        s3.delete_object(Bucket=bucket, Key=file_name)