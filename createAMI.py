import boto3
from pprint import pprint
from datetime import datetime

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
        now = datetime.today().strftime('%Y-%m-%d')
        Name = instance_id + '-AMI-' + str(now)
        image = ec2.create_image(
            InstanceId=instance_id,
            NoReboot=True,
            Name=Name
        )
