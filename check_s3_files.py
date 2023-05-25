import boto3
from botocore.exceptions import ClientError
import datetime
from datetime import timedelta, date
import logging,re, os
from dotenv import load_dotenv

load_dotenv()

#Setup logger
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')

#Setup profile to use in this session
boto3.setup_default_session(profile_name=os.getenv("PROFILE_NAME"))

#Setup boto3 client
s3_client = boto3.client('s3')

#Setup region
region = os.getenv("REGION")

#the number of days an object will be considered expired
expire_threshold = int(os.getenv("EXPIRE_THRESHOLD"))

#the total number of s3 objects that will be removed
total_objects = 0

expiring_folders = []

#bucket Name
bucket_name = os.getenv("BUCKET_NAME")
s3_lifecycle = os.getenv("S3_LIFECYCLE")

#Get 14 days ahead of today
date_in_two_weeks = date.today() + timedelta(days=14)
logging.info(f"Getting the dates in 14 days: {date_in_two_weeks}")

#Get date 3 years before the date
cut_date = date_in_two_weeks - timedelta(days=expire_threshold)
logging.info(f"Files older than {cut_date} will be considered expired")

def check_number_of_objects(folder_names):
    file_count=0
    folder_names = folder_names
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for file in bucket.objects.filter(Prefix=folder_names):
        file_count+=1
    return file_count

def check_subdirectories_older_than_date(s3client,logs_file, cut_date):
    global total_objects
    global expiring_folders
    prefix = logs_file+"/"
    result = s3client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    for o in result.get('CommonPrefixes'):
        subfolder = o.get('Prefix')
        
        # Use regex to find the dates in the folder structure
        regex = r"(?<=dt=)\d+"
        match = re.search(regex, subfolder)
        if match:
            dates_in_log_folder = match.group(0)
            dates_in_log_folder_object = datetime.datetime.strptime(dates_in_log_folder,"%Y%m%d")
            
            #Check if folder than is going to be older than cut-off date
            if dates_in_log_folder_object.date() <= cut_date:
                num_of_files = check_number_of_objects(subfolder)
                logging.info(f"This folder will be expired by {cut_date} : {subfolder}, which contains {num_of_files} objects" )
                expiring_folders.append(subfolder)
                total_objects += num_of_files
        else:
            logging.info(f"No folders with dates found in {logs_file}")
            
def publish_message(topic_arn, message, subject, region):
    """
    Publishes a message to a topic.
    """
    AWS_REGION = region
    sns_client = boto3.client('sns', region_name=AWS_REGION)
    
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject,
        )['MessageId']
    except ClientError:
        logging.exception(f'Could not publish message to the topic.')
        raise
    else:
        return response

if __name__ == "__main__":
    # Get the content of logs.directory and get the files
    with open("logs_directory.txt") as f:
        logs_directory = [line.rstrip() for line in f]
        
    # Loop through all the logs directories in S3
    for log_type in logs_directory:
        s3client = s3_client
        logging.info(f"Checking the folders inside {log_type}")
        check_subdirectories_older_than_date(s3client,log_type,cut_date)

    #Total objects will be expired in the next 14 days
    logging.info(f"A total of {total_objects:,} objects will be expired by {cut_date}")
    
    #Send SNS message
    topic_arn = os.getenv("TOPIC_ARN")
    message = f'This is a notification about logs files deletion.\n' \
            f'{total_objects:,} objects will be removed by {cut_date} (14 days from today).\n' \
            f'These folders and its content will be expiring by {cut_date} : \n'
    
    for folders in expiring_folders:
        message += folders + "\n"
        
    message += f'Checkout the bucket S3 lifecycle here {s3_lifecycle}.'
    
    subject = 'Logs will be removed in S3 logs [TESTING]'
    logging.info(f'Publishing message to topic - {topic_arn}...')
    message_id = publish_message(topic_arn, message, subject,region)
    logging.info(
        f'Message published to topic - {topic_arn} with message Id - {message_id}.'
    )
