import json
import boto3
import os
from constants.aws_config import aws_secret_access_key,aws_access_key_id
from helpers.custom_logger import enable_custom_logging

s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key,region_name='us-east-1')
textract = boto3.client('textract',region_name='us-east-1',
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
enable_custom_logging()
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
event = {'Records': [{'EventSource': 'aws:sns', 'EventVersion': '1.0', 'EventSubscriptionArn': 'arn:aws:sns:us-east-1:833984991867:AmazonTextract-lambda:621bdf14-1ad2-49d3-a41f-da6122240d20', 'Sns': {'Type': 'Notification', 'MessageId': 'c509b577-276e-5571-a3e1-d4596362002b', 'TopicArn': 'arn:aws:sns:us-east-1:833984991867:AmazonTextract-lambda', 'Message': '{"JobId":"693dc445a2a03c4e7ee04ad97beb994a0d3d17806a1bd862e358c731f6ebbec4","Status":"SUCCEEDED","API":"StartDocumentTextDetection","JobTag":"pdf_text","Timestamp":1756643788910,"DocumentLocation":{"S3ObjectName":"devoted/zai_medical_records_pipeline/concat-files/digitized-pdf/ed4b9177-3f4f-4aca-a203-3ce25b499bd1_AJX27GW4KC_IP1_20250703_152838.pdf","S3Bucket":"zai-revmax-qa"}}', 'Timestamp': '2025-08-31T12:36:28.972Z', 'SignatureVersion': '1', 'Signature': 'Cjee99sooRFKrA6AHiVKASdD6gsqZhKvDUpGAp9zBW+n/8mLl7wprhZGXB1c4VXArx7p/kYyvopeV2RUoQl+j/D9uCd2NvhUeVda82AytCcAMBjM9Ypi2ZsfJuVd4yXgLXLP6l9cchVrVpsgzjj5MIjwWtIWfy8iX8AqOV1VFT7+CxuPNqwT4EGgPmORexNtN3hmlOCXZ14twSAzNuuOg7aSEiSgmxwZ9f+qHI1AvcF2BvOGsp4RQ9iVTosT8CUC846a3LgI5KktH9fjjP2DyKR1HoKJkcASnT+ac0tkaRrafbzjK24kTMgI/bOJoFdDOg0ZhvA6jr4UIUFc7So0gw==', 'SigningCertUrl': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-6209c161c6221fdf56ec1eb5c821d112.pem', 'Subject': None, 'UnsubscribeUrl': 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:833984991867:AmazonTextract-lambda:621bdf14-1ad2-49d3-a41f-da6122240d20', 'MessageAttributes': {}}}]}
lambda_handler(event=event,context='')