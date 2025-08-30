import boto3
import pandas as pd
from datetime import datetime

def get_log_streams(cloudwatch, LOG_GROUP_NAME, start_time, end_time):
    log_streams = []
    log_stream_ls = []
    response = cloudwatch.describe_log_streams(
            logGroupName=LOG_GROUP_NAME,
            orderBy='LastEventTime',
            descending=True
    )
    log_streams.extend(response['logStreams'])
    while 'nextToken' in response:
        response = cloudwatch.describe_log_streams(
            logGroupName=LOG_GROUP_NAME,
            orderBy='LastEventTime',
            descending=True,
            nextToken=response['nextToken']
        )
        log_streams.extend(response['logStreams'])
    for log_stream in log_streams:
        if 'lastEventTimestamp' in log_stream and start_time <= log_stream['lastEventTimestamp'] <= end_time:
            # print(log_stream, log_stream['logStreamName'])
            log_stream_ls.append(log_stream['logStreamName'])
    
    return log_stream_ls

def get_log_events(cloudwatch, log_group_name, log_stream_name):
    # client = boto3.client('logs')
    events = []
    next_token = None

    while True:
        if next_token:
            response = cloudwatch.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                startFromHead=True,
                nextToken=next_token
            )
        else:
            response = cloudwatch.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                startFromHead=True
            )

        events.extend(response.get("events", []))
        next_token = response.get("nextForwardToken")

        # Break the loop if there are no more events
        if not next_token or next_token == response.get("nextForwardToken"):
            break

    # Convert timestamps and create DataFrame
    ls = []
    for event in events:
        timestamp = event["timestamp"]
        formatted_time = datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
        # print(formatted_time, event["message"])
        ls.append((formatted_time, event["message"]))

    df = pd.DataFrame(ls, columns=['TimeStamp', 'Message'])
    return df