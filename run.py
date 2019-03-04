import random
import shutil
import os
import subprocess
import gzip
import datetime
import calendar
import decimal
#import time

import boto3  # AWS SDK
from faker import Faker

s3_resource = boto3.resource('s3')
fake = Faker() # Generate random date for given month. 
year_start = 2010
year_end = 2013
orders_per_month = 10000
max_lines_per_order = 10
data_folder = 'partitioned-data'
s3_bucket = "werberm-bigdata"
crawler_name = "s3-partitioned-crawler"
compress_output_data = False

def empty_directory(target):
    
    print('Deleting contents of ./{}/ ...'.format(target))
    for dirpath,dirs,files in os.walk(target):
        if dirpath != target:
            shutil.rmtree(dirpath)
            print('deleted ./{}/'.format(dirpath))


def create_dir_if_not_exist(target):

    if not os.path.exists(target):
        os.makedirs(target)


def get_random_date_string(year, month):

    days_in_month = calendar.monthrange(year, month)[1]
    upper_rand_range = days_in_month + 1
    day = random.randrange(1,upper_rand_range)
    my_date_string = "{}-{}-{}".format(year, 
        f'{month:02}', 
        f'{day:02}')
    return my_date_string


def generate_order_data():
    
    order_id = 1

    for year in range(year_start, year_end):
        for month in range(1, 13):

            partition_path = "year={}/month={}".format(
                year, month
                )

            local_order_dir = "{}/orders/{}".format(
                data_folder, partition_path)
            local_order_line_dir = "{}/order_lines/{}".format(
                data_folder, partition_path)

            create_dir_if_not_exist(local_order_dir)
            create_dir_if_not_exist(local_order_line_dir)
            
            order_file_path = "{}/data.csv".format(local_order_dir)
            order_line_file_path = "{}/data.csv".format(local_order_line_dir)
            
            order_file = None
            order_line_file = None

            if (compress_output_data == True):
                order_file_path += ".gz"
                order_line_file_path += ".gz"
                order_file = gzip.open(order_file_path,'wt')
                order_file = gzip.open(order_line_file_path,'wt')
            else:
                order_file = open(order_file_path,'w')
                order_line_file = open(order_line_file_path,'w')
                
            order_file.write(
                "\"order_id\"|\"order_date\"|\"order_status\"\n"
                )
            order_line_file.write(
                "\"order_id\"|\"line_id\"|\"product_id\"|" 
                + "\"quantity\"|\"unit_price\"|\"extended_price\"\n"
                )
            print('Writing orders for {}-{} ...'.format(
                year, month
                )
            )

            for row in range(1, orders_per_month):
                # write the order header
                order_date = get_random_date_string(year, month)
                order_status   = 'complete'
                row = "{}|{}|{}\n".format(
                    order_id, 
                    order_date, 
                    order_status
                    )
                order_file.write(row)
                order_id += 1

                number_of_order_lines = random.randrange(1, max_lines_per_order)

                # write the order lines
                for line_id in range (1, number_of_order_lines + 1):
                    product_id = random.randrange(1,10000)
                    unit_price = (decimal.Decimal(
                        random.randrange(100, 9999))
                        / 100
                    )
                    quantity = random.randrange(1,11)
                    extended_price = quantity * unit_price
                    row = "{}|{}|{}|{}|{}|{}\n".format(
                        order_id, 
                        line_id, 
                        product_id,
                        quantity,
                        unit_price,
                        extended_price
                        )
                    order_line_file.write(row)

            order_file.close()
            order_line_file.close()

def delete_old_data_from_s3():
    os.system("aws s3 rm s3://{}/{} --recursive".format(
        s3_bucket, data_folder
        )
    )


def sync_new_data_to_s3():
    os.system("aws s3 sync ./{} s3://{}/{}".format(
        data_folder, s3_bucket, data_folder
        )
    )


def start_glue_crawler(crawler):
    print('Starting glue crawler...')
    os.system("aws glue start-crawler --name {}".format(crawler))
    print('Crawler started.')


def main():
    empty_directory(data_folder)
    generate_order_data()
    delete_old_data_from_s3()
    sync_new_data_to_s3()
    start_glue_crawler(crawler_name)


main()