import json
import pandas as pd
import boto3
import zipfile
import io
import re
import gzip
import os 
import urllib.parse 
from datetime import datetime
import pytz
import logging
import ijson
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')



lambda_client = boto3.client('lambda')
lambdaLogStreamName=os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
lambdaLogGroupName=os.environ['AWS_LAMBDA_LOG_GROUP_NAME']

FILE_PROCESSING_TYPE =  {
    #'australian_heritage_database':'LANDINSIGHTS'
    }
FILE_CLASSIFICATION = {
    'li_audit_reports':'contamination_audit_reports'
    ,'li_clr':'contamination_land_register'
    ,'li_licences_approvals':'contamination_licenses_approvals'
    ,'national_pollutant_inventory':'contamination_national_pollutant_inventory'
    ,'li_notices':'contamination_notices'
    ,'pfas':'contamination_pfas'
    ,'li_pca_regulated':'contamination_potentially_contaminated_activities_regulated'
    ,'australian_heritage':'heritage_australian_heritage_database'
    ,'local_state_heritage':'heritage_local_state_heritage'
    ,'envrisk_nsw':'environmental_insights'
}

LANDING_BUCKET = os.environ['LANDING_BUCKET']
RAW_BUCKET = os.environ['RAW_BUCKET'] 

PREFIX_INCOMING = { 'LANDINSIGHTS':'Incoming'
    
}


PREFIX_PROCESSED = { 'LANDINSIGHTS':'Processed'
    
}


PREFIX_ARCHIVE = { 'LANDINSIGHTS':'Archived'
    
}

aest = pytz.timezone('Australia/Sydney')
DATETIMEEXTENSION = datetime.now(aest).strftime("%Y%m%d_%H%M%S")
DATEEXTENSION = datetime.now(aest).strftime("%Y%m%d")
gv_accepted_file_ext = ['geojson','json'] 
# BUCKET_ARCHIVE =
# PREFIX_ARCHIVE = 
class FakeFloat(float):
    def __init__(self, value):
        self._value = value
    def __repr__(self):
        return str(self._value)

# so json.dumps knows what to do with Decimal
def DecimalEncoder(o):
    if isinstance(o, decimal.Decimal):
        # Subclass float with custom repr?
        return FakeFloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")
    
def send_slack_notification(msg):
    payload = {'LOG_GROUP':lambdaLogGroupName, 'LOG_STREAM_NAME':lambdaLogStreamName, 'SLACK_MSG':msg }
    
    invoke_response = lambda_client.invoke(FunctionName="onedata-slack-log",
                                           InvocationType='Event',
                                           Payload=json.dumps(payload).encode('utf-8'))

def copy_s3_object( LV_BUCKET_FROM, LV_KEY_FROM, LV_BUCKET_TO, LV_KEY_TO):
    
    #### Copy file from LV_BUCKET_FROM/LV_KEY_FROM to LV_BUCKET_INCOMING/LV_KEY_INCOMING, the KEYS contain the PREFIX, and the filenames passed from handler()
    copy_source = {
    'Bucket': LV_BUCKET_FROM,
    'Key': LV_KEY_FROM
    }


       
    s3.meta.client.copy(copy_source, LV_BUCKET_TO,LV_KEY_TO)
    

  
def process_s3_object(lv_process,lv_file_pattern,FILENAME, LV_BUCKET_INCOMING, LV_KEY_INCOMING, LV_BUCKET_PROCESSED, LV_PREFIX_PROCESSED):
    
    
    ####Read the zip file from bucket from where event is generated
    if  lv_process == 'LANDINSIGHTS':
         
        zip_obj = s3.Object(bucket_name=LV_BUCKET_INCOMING, key=LV_KEY_INCOMING)
        buffer = io.BytesIO(zip_obj.get()["Body"].read())
        z = zipfile.ZipFile(buffer)
        i = 0
        for lv_filename in z.namelist():
            inmem = io.BytesIO()
            with gzip.GzipFile(fileobj=inmem, mode='wb') as fh: 
                with io.TextIOWrapper(fh, encoding='utf-8') as wrapper:
                    with z.open(lv_filename) as f:
                 
                        parser = ijson.parse(f)
                        _ = wrapper.write('[\n')
                        pval = False
                        for prefix, event, value in parser:
                            # only interested in the feature array elements, which are maps, initialise stuff
                            if (prefix, event) == ('features.item', 'start_map'):
                                objects= [{}]
                                keys = [None]
                                grp_offset = None
                                pval = True
                                
                            # do anything that needs to happen once we reach the end of a feature, ie write the element
                           
                            elif (prefix, event) == ('features.item', 'end_map'):
                                pval = False
                                if i:
                                    # don't write a comma on the zeroth (first) line
                                    _ = wrapper.write('\n,')
                                _ = wrapper.write(json.dumps(objects[-1], default=DecimalEncoder))
                                i = i + 1
                            # whilst we are inside the "map" build the object that we will need to write in the branch above
                            elif pval:
                                if event == 'start_map':
                                    # this is the start of a sub map (dict)
                                    keys.append(grp_offset)
                                    grp_offset = None
                                    objects.append({})
                                elif event == 'start_array':
                                    # this is the start of a sub list
                                    keys.append(grp_offset)
                                    grp_offset = None
                                    objects.append([])
                                elif event.startswith('end_'):
                                    # The sub list of map completed, so attach it to the parent
                                    newobj = objects.pop()
                                    grp_offset = keys.pop()
                                    if grp_offset:
                                        objects[-1][grp_offset] = newobj
                                    else:
                                        objects[-1].append(newobj)
                                elif event == 'map_key':
                                    # We have a key for a value, so save it
                                    grp_offset = value
                                else:
                                    # This is the value
                                    if grp_offset:
                                        # for a key
                                        objects[-1][grp_offset] = value 
                                    else:
                                        # or, to append to an existing list
                                        objects[-1].append(value)
                        
                        # finally, close the list
                        _ = wrapper.write('\n]')
                    # Now we need to save out gzip blob 
                _=inmem.seek(0) 
                lv_process_file_name = f'{os.path.splitext(lv_filename)[0]}_{DATETIMEEXTENSION}{os.path.splitext(lv_filename)[1]}.gz'
                LV_KEY_PROCESSED = f'{LV_PREFIX_PROCESSED}/{lv_process_file_name}'
                s3_client.put_object(Bucket=LV_BUCKET_PROCESSED, Body=inmem , Key=LV_KEY_PROCESSED)
                
    
    else:
        lv_process_file_name=f'{os.path.splitext(FILENAME)[0]}_{DATETIMEEXTENSION}{os.path.splitext(FILENAME)[1]}'
        LV_KEY_PROCESSED = f'{LV_PREFIX_PROCESSED}/{lv_process_file_name}'
        lv_processed_file_list = [lv_process_file_name]
        copy_source = {
            'Bucket': LV_BUCKET_INCOMING,
            'Key': LV_KEY_INCOMING
            }
            
        s3.meta.client.copy(copy_source, LV_BUCKET_PROCESSED,LV_KEY_PROCESSED)
                
           
    return 0       
            
                
    
    

def gzip_file(body,LV_BUCKET_PROCESSED, LV_KEY_PROCESSED):
    
    inmem = io.BytesIO()
    with gzip.GzipFile(fileobj=inmem, mode='wb') as fh: 
        with io.TextIOWrapper(fh, encoding='utf-8') as wrapper:
            wrapper.write(json.dumps(body, ensure_ascii=False, default=None))
    inmem.seek(0)
    s3_client.put_object(Bucket=LV_BUCKET_PROCESSED, Body=inmem, Key=LV_KEY_PROCESSED)
    
     
    

    
    
def delete_s3_object(BUCKET,KEY):
    s3.Object(BUCKET, KEY).delete()

def lambda_handler(event, context):
    
    # ## Reading the bucket and key from where the s3 event generated
    ## BUCKET_FROM = event['Records'][0]['s3']['bucket']['name'] 
    ##  KEY_FROM = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')  
    
    # Reading the bucket and key from the event bridge event
    BUCKET_FROM = urllib.parse.unquote_plus(event[ 'detail']['bucket']['name'], encoding='utf-8') 
    KEY_FROM = urllib.parse.unquote_plus(event[ 'detail']['object']['key'], encoding='utf-8') 
     
    # # ##test event values###
    # BUCKET_FROM = 'external-storage-land-insights-481949735920-ap-southeast-2'
    # KEY_FROM = 'Envrisk_NSW_V4.geojson.zip' 
     
    FILENAME = os.path.basename(KEY_FROM)
        
    file_modified_date=s3_client.head_object(Bucket=BUCKET_FROM, Key=KEY_FROM)  
    lv_year=file_modified_date["LastModified"].strftime('%Y')         
    lv_month=file_modified_date["LastModified"].strftime('%m')        
    lv_day=file_modified_date["LastModified"].strftime('%d')          
    
    try:      
        
        logger.info(f'****STARTED: Processing file {BUCKET_FROM}/{KEY_FROM}**********')    
    
        name = [ value for key, value in FILE_PROCESSING_TYPE.items() if FILENAME.lower().startswith(key)  ]
        if name == []:
            name = ['LANDINSIGHTS'] 
        
        file_pattern = [ key for key, value in FILE_CLASSIFICATION.items() if FILENAME.lower().startswith(key)  ]
        no_folder_lv = 0
        if file_pattern == []:
            no_folder_lv = 1
            raise Exception('The file name doesnt match any the file pattern specified')
        else:
            file_pattern = file_pattern[0]
            folder_name = FILE_CLASSIFICATION[file_pattern]
        
        lv_process = name[0]
        
        LV_BUCKET_INCOMING = RAW_BUCKET 
        LV_PREFIX_INCOMING = f'{PREFIX_INCOMING[lv_process]}/{folder_name}'
        LV_KEY_INCOMING = f'{LV_PREFIX_INCOMING}/{FILENAME}'
        
    
    
        LV_BUCKET_PROCESSED = RAW_BUCKET
        LV_PREFIX_PROCESSED = f'{PREFIX_PROCESSED[lv_process]}/{folder_name}/{lv_year}/{lv_month}/{lv_day}' 
        # LV_PREFIX_PROCESSED = f'{PREFIX_PROCESSED[lv_process]}/{folder_name}' #kez-20231221 - commenting the folder structure to exclude date
       
          
        
        LV_BUCKET_ARCHIVE = RAW_BUCKET
        LV_PREFIX_ARCHIVE = f'{PREFIX_ARCHIVE[lv_process]}/{folder_name}'
        
    
        logger.info(f'##Started: Copying file from {BUCKET_FROM}/{KEY_FROM} to incoming folder {LV_BUCKET_INCOMING}/{LV_KEY_INCOMING}')
        copy_s3_object(BUCKET_FROM, KEY_FROM,LV_BUCKET_INCOMING,LV_KEY_INCOMING)
        logger.info(f'##Completed:Copying file to {LV_BUCKET_INCOMING}/{LV_KEY_INCOMING}')
        
        
        logger.info(f'##Started: Copying file to processing folder {LV_BUCKET_PROCESSED}/{LV_PREFIX_PROCESSED}')
        process_resp = process_s3_object(lv_process,file_pattern, FILENAME,LV_BUCKET_INCOMING, LV_KEY_INCOMING,LV_BUCKET_PROCESSED,LV_PREFIX_PROCESSED)
        if process_resp == 1 :
            lv_accepted_file_ext = ','.join(gv_accepted_file_ext)
            lv_accepted_file_pattern = list(FILE_CLASSIFICATION.keys())
            raise Exception(f'ERROR: The unzipped file extension is not accepted : {lv_accepted_file_ext} or the file name doesnt match any the file pattern specified {lv_accepted_file_pattern}')
        logger.info(f'##Completed: Copying file to processing folder {LV_BUCKET_PROCESSED}/{LV_PREFIX_PROCESSED}')
        
        
        logger.info(f'##Started: Copying file to archive folder {LV_BUCKET_ARCHIVE}/{LV_PREFIX_ARCHIVE}')
        lv_file_name = f'{os.path.splitext(FILENAME)[0]}_{DATEEXTENSION}{os.path.splitext(FILENAME)[1]}'
        LV_KEY_ARCHIVE = f'{LV_PREFIX_ARCHIVE}/{lv_file_name}'
        copy_s3_object(BUCKET_FROM,KEY_FROM,LV_BUCKET_ARCHIVE,LV_KEY_ARCHIVE)
        logger.info(f'##Completed: Copying file to archive folder {LV_BUCKET_ARCHIVE}/{LV_PREFIX_ARCHIVE}')
        
        
        logger.info(f'##Started: Deleting file from Landing bucket {BUCKET_FROM}/{KEY_FROM}')
        delete_s3_object(BUCKET_FROM,KEY_FROM)
        logger.info(f'##Completed: Copying file to archive folder {BUCKET_FROM}/{KEY_FROM}')
         
    except Exception as e:
        
        logger.info(f'*********FAILED: Processing file {BUCKET_FROM}/{KEY_FROM}*********') 
        logger.exception(str(e))
        msg = f'FAILED: Processing file {BUCKET_FROM}/{KEY_FROM}'
        send_slack_notification(msg)
        raise Exception(str(e))
        
        
        return {
            'statusCode': 401,
            'body': json.dumps(f'FAILED: Landinsight file processing !')
        }
        
        
        
                                           
    return {
        'statusCode': 200,
        'body': json.dumps('SUCCESS: LandInsight file processing finished !')
    }



