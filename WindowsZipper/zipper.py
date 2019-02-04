import os
import datetime
import time
import shutil
from zipfile import ZipFile 


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


def zipper():
    now = time.time()
    to_zip = []
    file_path = get_all_file_paths('.')
    print('Following files will be zipped:')
    for file in file_path:
        if os.stat(file).st_mtime < now - 15 * 86400:
            print(file)
            to_zip.append(file)
    file_name = "logs_%s" %(str(datetime.datetime.today().strftime('%Y-%m-%d')))
    with ZipFile(file_name,'w') as zip:
        for zip_file in to_zip:         
            zip.write(zip_file) 
  
    print('All files zipped successfully!')
    shutil.move(file_name, 'C:\\Users\\rohan\\Desktop\\test')



zipper()