pagintor = s3.get_paginator('list_objects')
pages = paginator.paginate(Bucket=bucket)
for page in pages:
    for obj in page['Contents']:
        