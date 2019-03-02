
import random
import boto3
import shutil
import os
import subprocess
import gzip

year_start = 2010
year_end = 2016
rows_per_file = 100000

data_folder  = 'partitioned-data'
s3_resource  = boto3.resource('s3')
s3_bucket    = "werberm-bigdata"
crawler_name = "s3-partitioned-crawler"
file_name    = "data.csv.gz"

# delete old data, if any
for dirpath, dirnames, filenames in os.walk(data_folder):
    if dirpath != data_folder:
        shutil.rmtree(dirpath)
        print('deleted local directory {}'.format(dirpath))

for year in range(year_start, year_end):
    for month in range(1, 13):

        local_path = "{}/orders/year={}/month={}".format(data_folder, year, month)
        file_path = "{}/{}".format(local_path, file_name)
        
        if not os.path.exists(local_path):
            os.makedirs(local_path)
            print('Created local direcory {}'.format(local_path))
    
        file = gzip.open(file_path,'wt')
        
        # write column headers
        file.write("\"extended_price\"|\"order_status\"\n");
        
        for row in range(1, rows_per_file):
            extended_price = random.randint(1, 10000)
            order_status   = 'complete'
            row = "{}|{}\n".format(extended_price, order_status)
            file.write(row)
        
        print('Wrote {} records...'.format(rows_per_file))
        file.close()

os.system("aws s3 rm s3://{}/{} --recursive".format(s3_bucket, data_folder))
os.system("aws s3 sync ./{} s3://{}/partitioned-data".format(data_folder, s3_bucket, data_folder))
print('Starting glue crawler...')
os.system("aws glue start-crawler --name {}".format(crawler_name))
print('Crawler started.')