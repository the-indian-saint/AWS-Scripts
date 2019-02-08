tagname="Project"
tagvalue="Scrum0"
log=""
next_token=''
log=log+"Description"+";"+"Encrypted"+";"+"OwnerId"+";"+"Progress"+";"+"SnapshotId"+";"+"State"+";"+"VolumeId"+";"+"VolumeSize"+";"+"Project Tag"+"\n"
i=0

while next_token is not None:
    i=i+1
    print(i)
    response = ec2.describe_snapshots(
                Filters=[
            {
                    'Name': 'description',
                    'Values': ["*",]
            },
            ],
        MaxResults=1000,
        NextToken=next_token
    )
    #print(response)
    for snap in response['Snapshots']:
        Description=snap['Description']
        Encrypted=str(snap['Encrypted'])        
        OwnerId=str(snap['OwnerId'])
        Progress=str(snap['Progress'])
        SnapshotId=str(snap['SnapshotId'])
        
        State=snap['State']
        
        VolumeId=str(snap['VolumeId'])
        VolumeSize=str(snap['VolumeSize'])
        StartTime=str(snap['StartTime'])
        value=''
        try:
            for tag in snap['Tags']:
                #print(tag)
                if(tag['Key']=="Project"):
                    value=tag['Value']
                    #print(value)
        except Exception as e:
            #print(e)
            a=1
        #print(Description)
        log=log+Description+";"+Encrypted+";"+OwnerId+";"+Progress+";"+SnapshotId+";"+State+";"+VolumeId+";"+VolumeSize+";"+value+";"+StartTime+"\n"
    try:
        next_token=response['NextToken']
    except Exception as e:
        next_token=None
