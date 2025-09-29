import json
import boto3
import os
from constants.aws_config import aws_secret_access_key,aws_access_key_id
from helpers.custom_logger import S3Logger
import pandas as pd


s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key,region_name='us-east-1')
textract = boto3.client('textract',region_name='us-east-1',
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
logger = S3Logger()
def getJobResultForText(jobId):
    pages = []
    
    response = textract.get_document_text_detection(JobId=jobId)
    pages.append(response)

    nextToken = None
    if ('NextToken' in response):
        nextToken = response['NextToken']

    while (nextToken):

        response = textract.get_document_text_detection(
            JobId=jobId, NextToken=nextToken)
        pages.append(response)
        nextToken = None

        if ('NextToken' in response):
            nextToken = response['NextToken']

    return pages


def getJobResultForTable(jobId):
    maxResults = 1000
    paginationToken = None
    finished = False
    pages = []

    while finished == False:
        response = None
        if paginationToken == None:
            response = textract.get_document_analysis(
                JobId=jobId, MaxResults=maxResults)
        else:
            response = textract.get_document_analysis(
                JobId=jobId, MaxResults=maxResults, NextToken=paginationToken)
        pages.append(response)
        print('Document Detected.')
        if 'NextToken' in response:
            paginationToken = response['NextToken']
        else:
            finished = True

    return pages

def lambda_handler(event, context):
    print('started 2nd lambda...', type(event))
    print(event)
    key = eval(json.loads(json.dumps(event))[
        'Records'][0]['Sns']['Message'])['DocumentLocation']['S3ObjectName']
    document_name = key.split('/')[-1]
    prefix = key.split("/concat-files")[0]
    notificationMessage = json.loads(json.dumps(event))[
        'Records'][0]['Sns']['Message']
    pdfTextTableExtractionStatus = json.loads(notificationMessage)['Status']
    pdfTextTableExtractionJobTag = json.loads(notificationMessage)['JobTag']
    pdfTextTableExtractionJobId = json.loads(notificationMessage)['JobId']
    pdfTextTableExtractionDocumentLocation = json.loads(notificationMessage)[
        'DocumentLocation']

    pdfTextTableExtractionS3ObjectName = json.loads(json.dumps(
        pdfTextTableExtractionDocumentLocation))['S3ObjectName']
    pdfTextTableExtractionS3Bucket = json.loads(json.dumps(
        pdfTextTableExtractionDocumentLocation))['S3Bucket']

    print(pdfTextTableExtractionS3ObjectName + ' : ' + pdfTextTableExtractionStatus)
    print(pdfTextTableExtractionJobTag, '|', 'loc:', pdfTextTableExtractionDocumentLocation)

    if (pdfTextTableExtractionStatus == 'SUCCEEDED'):
        if pdfTextTableExtractionJobTag == 'pdf_text':
            print(f"text extraction completed:- {document_name}")

            print(f"collecting textract results:- {document_name}")
        
            response = getJobResultForText(pdfTextTableExtractionJobId)
            # line number
            # n = 1
            # for resultPage in raw_json:
            #     for item in resultPage["Blocks"]:
            #         if item["BlockType"] == "LINE":
            #             item["LINE_NUMBER"] = n
            #             n+=1
            count = 1
            n = 1
            for ele in response:
                for blk in  ele.get('Blocks')[1:]:
                    if blk.get('Page') == count +1:
                        count = count +1
                        n=1
                    elif blk.get('BlockType') == 'LINE':
                        blk.update({'LINE_NUMBER':f'{n}'})
                        n+=1
            raw_json = json.dumps(response)
                        
            # outputJSONFileName = os.path.splitext(
            #     pdfTextTableExtractionS3ObjectName)[0] + '.json'
            outputJSONFileName = pdfTextTableExtractionS3ObjectName.split('/')[-1].replace('.pdf', '.json')
            print(f"textract response is being saved:- {document_name}")
            s3.put_object(Body=raw_json, Bucket=pdfTextTableExtractionS3Bucket,
                        Key=f'{prefix}/textract-response/raw-json/{outputJSONFileName}')
            print(f"textract response is saved:- {document_name}")
            
            
            
        elif pdfTextTableExtractionJobTag == 'pdf_table':
            print(f"table extraction completed:- {document_name}")

            print(f"collecting table extraction results:- {document_name}")

            response = getJobResultForTable(pdfTextTableExtractionJobId)
            raw_json = json.dumps(response)
    
            # outputJSONFileName = os.path.splitext(
            #     pdfTextTableExtractionS3ObjectName)[0] + '_table_merged.json'
            outputJSONFileName = pdfTextTableExtractionS3ObjectName.split('/')[-1].replace('.pdf', '.json')
            print(f"table extraction response is being saved:- {document_name}")

            s3.put_object(Body=raw_json, Bucket=pdfTextTableExtractionS3Bucket,
                          Key=f'{prefix}/textract-response/table-json/{outputJSONFileName}')
            print(f"table extraction response is saved:- {document_name}")
            
        else:
            print("unknown job tag")
        print(pdfTextTableExtractionStatus)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "response saved",
            "tag": f"{pdfTextTableExtractionJobTag}"
        }),
    }
event = {}
lambda_handler(event=event,context='')