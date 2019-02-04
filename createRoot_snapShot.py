#The below script fetches the root vloume ids from instance tags and creates a snapshot of those root volumes.

import boto3
from pprint import pprint

#use wscli to configure the access & secret key id.
#change the tag values in lines 10,12 & 31, 32

ec2 = boto3.client('ec2')

instances = ec2.describe_instances(
    Filters=[{
        'Name': 'tag:Owner',
        'Values':[
            'Rohan'
        ]

    }
    ]
)

for instances in instances['Reservations']:
    for instance in instances['Instances']:
        instance_id = instance['InstanceId']
        #use [0] to take snapshot of root volume, remove it and loop through the list BlockDeviceMappings if you want to take snapshots of all volumes.
        volume_id = instance['BlockDeviceMappings'][0]['Ebs']['VolumeId']
        ec2.create_snapshot(
            VolumeId = volume_id,
            TagSpecifications=[
                {
                'ResourceType': 'snapshot',
                'Tags': [
                {
                    'Key': 'Owner',
                    'Value': 'Rohan'
                }
            ]
                }
            ]
        )