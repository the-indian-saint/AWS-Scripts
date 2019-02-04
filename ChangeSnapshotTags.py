import boto3

aws_access_key_id = ""  #your access key

aws_secret_key_id = ""  #your secret key

#change values in lines 15,17 30 and 31.

region = "us-east-1"


ec2 = boto3.client('ec2', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_key_id, region_name = region )


snapshots = ec2.describe_snapshots(
    Filters=[{
        'Name': 'tag:owner',     
        'Values':[
            'rohan'        
        ]
    }
    ]
)
    
for snapshot in snapshots['Snapshots']:
    ec2client = boto3.resource('ec2', region_name = region, aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_key_id )
    client = ec2client.Snapshot(snapshot['SnapshotId'])
    tag = client.create_tags(
        Tags=[
            {
                'Key': 'Owner3',
                'Value': 'Aakash'
            }
        ]
    )


