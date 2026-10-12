import boto3
import json
import time
from datetime import datetime
from datetime import timedelta
import requests
import os
import re
# import s3fs

client = boto3.client('logs')
alert_url = os.environ['SLACK_CHANNEL'] 
# ExtraArgs = {'ServerSideEncryption': 'AES256'}
# s3 = s3fs.S3FileSystem(s3_additional_kwargs=ExtraArgs)

def slack_notify(title, body=None):
    
     
    if not isinstance(body, str):
        body = str(body)
    if (title):
        message_text = '\n*' + title + '*\n\n' + '```\n' + body.replace("\\n","\n").replace("\"","") + '\n```'
    else: 
        message_text = '```\n' + body.replace("\\n","\n").replace("\"","") + '\n```'
    slack_response = requests.post(alert_url, json={"text": message_text})
    return slack_response  
def get_full_logs(logGroup,logStreamName):
    print(logGroup)
    print(logStreamName)
    # latest_stream = client.describe_log_streams(logGroupName=group_name, orderBy='LastEventTime', descending=True,limit=1)
    latest_stream = client.describe_log_streams(logGroupName=logGroup,logStreamNamePrefix=logStreamName)
    latest_stream = latest_stream['logStreams']
        
    stream_name = latest_stream[0].get('logStreamName') 

    logs = client.get_log_events(logGroupName=logGroup,logStreamName=stream_name)
    return logs['events']
def clean_event(event):
    log_dict = {}
    log_dict['dt_time'] = dt_time = (datetime.fromtimestamp(event.get('timestamp')/1000)
                                     + timedelta(hours=10)).strftime('%d-%b-%Y %H:%M:%S')
    log_dict['message'] = clean_message(event.get('message'))
    return log_dict

def clean_message(message):
    message = re.sub('\[33m|\[32m|\[31m|\[0m','',message)
    message = re.sub('[0-9]{2}:[0-9]{2}:[0-9]{2}','',message)
    message = message.replace(u'\x1b','')
    message = message.replace(u'\xa0','')
#     message = message.strip()
    return message

def clean_logs(log_list):
    clean_list = []
    for event in log_list:
        log_dict = clean_event(event)
        if log_dict['message']:
            clean_list.append(log_dict)
    return clean_list

def process_logs(clean_logs):
    processed_list=[]
    for event in clean_logs:
        message = event['message']
        keep=True
        if ' of ' in message:
            keep=False
        elif len(message.strip()) == 0: #Only spaces
            keep=False
        elif 'Concurrency' in message:
            keep=False
        elif 'report_to_stepfunction' in message:
            keep=False
        elif 'Partial parse save file not found' in message:
            keep=False
        elif 'Configuration paths exist' in message:
            keep=False
        elif 'Unable to do partial parsing' in message:
            keep=False
        
        if keep:
            processed_list.append(event)
    return processed_list

def lambda_handler(event, context):
    time.sleep(20)
    log_group = event['LOG_GROUP']
    log_stream_name = event['LOG_STREAM_NAME']
    slack_notification_msg = event['SLACK_MSG']
    log_list = get_full_logs(log_group,log_stream_name)
    clean_list=clean_logs(log_list)
    processed_list = process_logs(clean_list)
    chunks = [processed_list[i:i + 34] for i in range(0, len(processed_list), 34)]
    chunks = [('\n'.join([f"{event['dt_time']} {event['message']}" for event in chunk])) for chunk in chunks]
    for i,log_str in enumerate(chunks):
        if i == 0:
            slack_notify(f'{slack_notification_msg}', log_str)
        else:
            slack_notify('', log_str)
    return {
        'statusCode': 200,
        'body': 'Success'
    }