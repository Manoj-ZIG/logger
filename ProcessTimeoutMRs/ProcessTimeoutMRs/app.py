import os
import json
import time
import boto3
import traceback
import urllib.parse
import pandas as pd
from io import BytesIO, StringIO
from datetime import datetime, timedelta

# from constants import instance_id, meta_dict
# from constants import lookBackTime, bucket_name
# from constants import queue, processing, processed, error
# from constants import vitals_lambda_script_path, postprocessing_lambda_script_path
# from constants import VITALS_LAMBDA_LOG_GROUP_NAME, POSTPROCESSING_LAMBDA_LOG_GROUP_NAME
# from constants import delete_object, copy_object, put_json_file, list_all_objects, read_json

try:
    from helpers.cloudwatch_constants import get_log_streams, get_log_events
    from constants.aws_config import aws_access_key_id, aws_secret_access_key
    from constants.constants import *
except ModuleNotFoundError as e:
    from .helpers.cloudwatch_constants import get_log_streams, get_log_events
    from .constants.aws_config import aws_access_key_id, aws_secret_access_key
    from .constants.constants import *

# temporary_session = boto3.Session(
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key, 
#     region_name='us-east-1'
# )
# cloudwatch = temporary_session.client('logs')
# ssm_client = boto3.client("ssm") 
    
def run_python_script_on_instance(ssm_client, instance_id, script_path, json_data, LOG_GROUP_NAME, file_path):
    
    print('Ec2 instance instantiated')
    #run the python script using aws system manager
    try:
        print(f'{script_path} is being executed')
        cmd=f'python3 {script_path} {json_data["Bucket"]} {file_path}'
        print(f'sending {cmd} command to ec2')
        response=ssm_client.send_command(
            InstanceIds = [instance_id],
            DocumentName = 'AWS-RunShellScript',
            Parameters = {
                "commands":[cmd]
            },
            CloudWatchOutputConfig={'CloudWatchLogGroupName': LOG_GROUP_NAME,'CloudWatchOutputEnabled': True}
        )
        print("sending response ",response)
        print("Response command Status ",response['Command']['Status'])
        print("Response command id ",response['Command']['CommandId'])
        return response['ResponseMetadata']['HTTPStatusCode']
    except: 
        print(traceback.format_exc())
        print('Exception occured while running python script')
        return None

def list_timed_out_mrs(log_stream_df, LOG_GROUP_NAME, search_string):
    '''
    ### Pre-Processes the dataframe and returns all the list of MRs that are timed out

    args:
        log_stream_df: dataframe
        LOG_GROUP_NAME: log group name
        search_string: string
    return:
        log_stream_df: dataframe
        to_be_processed_in_ec2: list
    '''
    to_be_processed_in_ec2 = []
    to_be_processed_in_ec2_dict = dict()
    if log_stream_df.shape[0] > 0:
        log_stream_df['StartStatus'] = log_stream_df['Message'].str.contains("START RequestId")
        log_stream_df['MRName'] = log_stream_df['Message'].apply(lambda x: str(x).split("|")[0].replace("processing: ", "") if "processing:" in str(x) else None)
        log_stream_df['MRKey'] = log_stream_df['Message'].apply(lambda x: str(x).replace("s3_key : ", "") if "s3_key" in str(x) else None)
        # Changed it from "Task timed out after" to "Status: timeout"
        log_stream_df['TimedOutStatus'] = log_stream_df['Message'].str.contains(search_string)
        print(f"Number of MRs triggered in {LOG_GROUP_NAME}: ", len(log_stream_df[log_stream_df['StartStatus'] == True].index))
        max = log_stream_df[log_stream_df['StartStatus'] == True].index.shape[0]
        groups = list(range(max))
        idx_ls = log_stream_df[log_stream_df['StartStatus'] == True].index
        log_stream_df.loc[idx_ls, 'Rank'] = groups
        log_stream_df['Rank'] = log_stream_df['Rank'].ffill()
        log_stream_df['Rank'] = log_stream_df['Rank'].bfill()
        rank_df = log_stream_df.groupby('Rank')['TimedOutStatus'].sum().reset_index()
        timed_out_ls = rank_df[rank_df['TimedOutStatus'] == 1]['Rank'].to_list()
        to_be_processed_in_ec2 = list(log_stream_df[(log_stream_df['Rank'].isin(timed_out_ls)) & (~log_stream_df['MRName'].isna())]['MRName'].unique())
        to_be_processed_in_ec2 = [i.replace(".csv", "") for i in to_be_processed_in_ec2]
        log_stream_df['MRName'] = log_stream_df.groupby(['Rank'])['MRName'].ffill()
        log_stream_df['MRKey'] = log_stream_df.groupby(['Rank'])['MRKey'].ffill()
        log_stream_df['MRName'] = log_stream_df.groupby(['Rank'])['MRName'].bfill()
        log_stream_df['MRKey'] = log_stream_df.groupby(['Rank'])['MRKey'].bfill()
        log_stream_df['MRName'] = log_stream_df['MRName'].apply(lambda x: str(x).replace(".csv", ""))
        log_stream_df['MRKey'] = log_stream_df['MRKey'].apply(lambda x: str(x).replace("\n", ""))
        to_be_processed_in_ec2_dict = log_stream_df[(log_stream_df['Rank'].isin(timed_out_ls))].groupby(['Rank']).agg({
            'MRName': lambda x: ','.join(set(x)),
            'MRKey': lambda x: ','.join(set(x)),   
        }).reset_index().drop(columns=['Rank']).set_index('MRName').to_dict()['MRKey']
    return log_stream_df, to_be_processed_in_ec2, to_be_processed_in_ec2_dict

def wait_for_instance(ec2c, ec2r, instance_id, region='us-east-1'):
    """
    To Turn On the EC2 instance
    """
    while True:
        response = ec2c.describe_instance_status(InstanceIds=[instance_id])
        statuses = response['InstanceStatuses']
        
        if len(statuses) == 0:
            print(f"Instance {instance_id} is not found or not in a state to be described.")
            instance = ec2r.Instance(instance_id)
            response = instance.start()
            time.sleep(20)
            continue
        
        state = statuses[0]['InstanceState']['Name']
        systemStatus = statuses[0]['SystemStatus']['Details'][0]['Status']
        InstanceStatus = statuses[0]['InstanceStatus']['Details'][0]['Status']
        if (state == 'running') and (systemStatus == 'passed') and (InstanceStatus == 'passed'):
            print(f"Instance {instance_id} is now running.")
            break
        else:
            print(f"Instance {instance_id} is in state '{state}'. Waiting...")
            time.sleep(5)

def lambda_handler(event, context):
    
    #s3c initialization
    try:
        s3c = boto3.client('s3', region_name='us-east-1')
        s3c.list_buckets()
        print("S3 client initialized successfully using IAM role in app.py.")
    except Exception as e:
        print(f"Failed to initialize S3 client with IAM role: {str(e)} in app.py.")
        if aws_access_key_id and aws_secret_access_key:
            s3c = boto3.client('s3', 
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)
            print("S3 client initialized successfully using manual keys in app.py.")
        else:
            raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in app.py.")
    
    #s3r initialization
    try:
        s3r = boto3.resource('s3', region_name='us-east-1')
        s3r.buckets.all()
        print("S3 resource initialized using IAM role in app.py.")
    except Exception as e:
        if aws_access_key_id and aws_secret_access_key:
            s3r = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            print("S3 resource initialized using manual keys in app.py.")
        else:
            raise Exception("S3 resource initialization failed. Check IAM role or AWS credentials in app.py.")
    
    # EC2 initialization
    if aws_access_key_id == ''  and aws_secret_access_key == '':
        ec2c = boto3.client('ec2', region_name='us-east-1')
        ec2r = boto3.resource('ec2', region_name='us-east-1')
        print("EC2 initialized using IAM role in app.py.")
    else:
        ec2r = boto3.resource('ec2', region_name='us-east-1', 
                             aws_access_key_id=aws_access_key_id, 
                             aws_secret_access_key=aws_secret_access_key)
        ec2c = boto3.client('ec2', region_name='us-east-1', 
                             aws_access_key_id=aws_access_key_id, 
                             aws_secret_access_key=aws_secret_access_key)
        print("EC2 initialized using manual keys in app.py.")
    
    # Cloudwatch initialization
    if aws_access_key_id == ''  and aws_secret_access_key == '':
        temporary_session = boto3.Session(
            region_name='us-east-1'
        )
        cloudwatch = temporary_session.client('logs')
        print("CloudWatch initialized using IAM role in app.py.")
    else:
        temporary_session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key, 
            region_name='us-east-1'
        )
        cloudwatch = temporary_session.client('logs')
        print("CloudWatch initialized using manual keys in app.py.")

    # SSM Client initialization
    if aws_access_key_id == ''  and aws_secret_access_key == '':
        ssm_client = boto3.client("ssm",
            region_name='us-east-1'
        )
        print("SSM client initialized using IAM role in app.py.")
    else:
        ssm_client = boto3.client("ssm",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key, 
            region_name='us-east-1'
        )
        print("SSM client  initialized using manual keys in app.py.")

    # LOG_GROUP_NAME_LS = [VITALS_LAMBDA_LOG_GROUP_NAME, POSTPROCESSING_LAMBDA_LOG_GROUP_NAME]
    environment = os.environ["PROCESSING_ENVIRONMENT"]
    instance_id = os.environ['INSTANCE_ID']
    PUBLISH_LOGS_TO = os.environ['PUBLISH_LOGS_TO']
    bucket_name = os.environ['BUCKET_NAME']
    meta_dict = {
        f"/aws/lambda/vitalLabExtraction-{environment}":"/home/revmaxai_data/vital_lab_extraction/vital_lab_extraction/app.py",
        f"/aws/lambda/rawDataPostprocess-{environment}":"/home/revmaxai_data/raw_data_postprocess/raw_data_postprocess/app.py"
        }
    if "source" in event:
        print("Triggered by Event Bridge")
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(minutes=lookBackTime)).timestamp() * 1000)
        all_queues, all_processing = [], []
        for LOG_GROUP_NAME, script_path in meta_dict.items():
            print(f"Checking for : {LOG_GROUP_NAME}")
            log_stream_ls = get_log_streams(cloudwatch, LOG_GROUP_NAME,start_time, end_time)
            lambda_name = LOG_GROUP_NAME.split('/')[-1]
            print(f"Total number of Log streams found for {lambda_name}: ", len(log_stream_ls))
            print(f"Started getting the logs for the log group {lambda_name}")
            log_stream_df = pd.DataFrame({})
            for log_stream in log_stream_ls:
                df = get_log_events(cloudwatch, LOG_GROUP_NAME, log_stream)
                log_stream_df = pd.concat([log_stream_df, df], ignore_index=True)
            search_string_ls = ['Status: timeout', 'Task timed out after']
            for search_string in search_string_ls:
                log_stream_df_res, to_be_processed_in_ec2, to_be_processed_in_ec2_dict = list_timed_out_mrs(log_stream_df.copy(), lambda_name, search_string)

                queue_keys, queue_files = list_all_objects(s3r, bucket_name, f"{queue}/{lambda_name}")
                processing_keys, processing_files = list_all_objects(s3r, bucket_name, f"{processing}/{lambda_name.split('-')[0]}")
                processed_keys, processed_files = list_all_objects(s3r, bucket_name, f"{processed}/{lambda_name}")
                error_keys, error_files = list_all_objects(s3r, bucket_name, f"{error}/{lambda_name}")
                # all_queues.extend(queue_files)
                all_processing.extend(processing_files)
                
                all_files = queue_files + processed_files + processing_files + error_files
                all_files = [i.replace(".json", "") for i in all_files]
                print(f"Total number of MRs timed out in {lambda_name} are ", len(to_be_processed_in_ec2))
                print(f"Timed out MRs list : ", to_be_processed_in_ec2)
                print(f"Timed out MRs key value : ", to_be_processed_in_ec2_dict)
                for MR, value in to_be_processed_in_ec2_dict.items():
                    if MR not in all_files:
                        json_file_name = MR + ".json"
                        json_data = {
                                "Bucket": bucket_name,
                                "Key": f"{value}",
                                "LambdaName": lambda_name,
                                "CompletedPath":f"{processed}/{lambda_name}/{json_file_name}",
                                "ErrorPath": f"{error}/{lambda_name}/{json_file_name}"
                        }
                        print("Creating a json file : ", json_data)
                        put_json_file(s3c, queue+"/"+lambda_name, json_file_name, json_data, bucket_name)

            queue_keys, queue_files = list_all_objects(s3r, bucket_name, f"{queue}/{lambda_name}")
            processing_keys, processing_files = list_all_objects(s3r, bucket_name, f"{processing}/{lambda_name.split('-')[0]}")
            all_queues.extend(queue_files)

            if len(processing_files) == 0 and len(queue_files) > 0:
                dest_key = processing+"/"+lambda_name.split("-")[0] + "/" + queue_files[0]
                copy_object(s3c, bucket_name, queue_keys[0], bucket_name, dest_key)
                delete_object(s3c, bucket_name, queue_keys[0])
                
                wait_for_instance(ec2c, ec2r, instance_id)
                # Read the json file and invoke ths script
                json_data = read_json(s3c, bucket_name, dest_key)
                print("Triggered in EC2 for : ", json_data['Key'], json_data['LambdaName'])
                status_code = run_python_script_on_instance(ssm_client, instance_id, script_path, json_data, PUBLISH_LOGS_TO, dest_key) 
                if status_code == 200:
                    all_processing.extend([queue_files[0]])
                    all_queues.remove(queue_files[0])
                
        print("In Queue : ", len(all_queues),  "In Processing : ", len(all_processing))
        if len(all_queues) == 0 and len(all_processing) == 0:
            instance_status=check_instance_status(ec2c, instance_id)
            print('INSTANCE_STATUS: ',instance_status)               
            if instance_status=="pending" or instance_status =='running':
                stop_response_ec2=ec2c.stop_instances(InstanceIds=[instance_id,],)
                ec2c.get_waiter('instance_stopped').wait(InstanceIds=[instance_id])
                print("EC2 instance stopped")
        
    elif "Records" in event and "Sns" in event['Records'][0]:
        print("Triggered by Sns")
        print(event)
        sns_event_message = eval(event['Records'][0]['Sns']['Message'])
        if sns_event_message['processing_status'] == 'Completed':
            json_file_path = sns_event_message['s3_file_path']
            json_data = read_json(s3c, bucket_name, json_file_path)
            copy_object(s3c, bucket_name, json_file_path, bucket_name, json_data['CompletedPath'])
            delete_object(s3c, bucket_name, json_file_path)
        
        elif sns_event_message['processing_status'] == 'Error':
            json_file_path = sns_event_message['s3_file_path']
            json_data = read_json(s3c, bucket_name, json_file_path)
            copy_object(s3c, bucket_name, json_file_path, bucket_name, json_data['ErrorPath'])
            delete_object(s3c, bucket_name, json_file_path)
        
        # isinstance_id = "i-04372e5806cec28b0"
        # bucket_name = "zai-revmax-qa"
        # key = "devoted/zai_medical_records_pipeline/medical-records-extract/sectioned-data/942bf061-a19d-434d-9b3f-d847c6cf0998-AJX28KZWKU-IP1/942bf061-a19d-434d-9b3f-d847c6cf0998_AJX28KZWKU_IP1_section_subsection.csv"
        # cmd = f'python3 {script_path} {bucket_name} {key}'
        # try:
        #     response = ssm_client.send_command(
        #         InstanceIds = [isinstance_id],
        #         DocumentName = 'AWS-RunShellScript',
        #         Parameters = {
        #             "commands":[cmd]
        #         },
        #         CloudWatchOutputConfig={'CloudWatchLogGroupName': '/aws/lambda/excerptsTest','CloudWatchOutputEnabled': True}
        #     )
        #     command_id = response['Command']['CommandId']
        #     print(response)
        #     print(f"Command submitted : {command_id}")
        #     # return {
        #     #     "statusCode": 200,
        #     #     "body" : json.dumps({"message":"Command submitted", "command_id":command_id}),
        #     # }
        # except Exception as e:
        #     print(f"Error : {str(e)}")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
