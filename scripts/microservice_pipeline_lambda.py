import json
import boto3

s3_client = boto3.client('s3')
glue_client = boto3.client('glue', region_name='us-east-1')

responseObject = {}
def lambda_handler(event, context):
    print('event:',event)
    #parse inputs from the API call 
    try:
        bucket = event.get('queryStringParameters').get('bucket')
        key = event.get('queryStringParameters').get('key')
    except AttributeError as e:
        msg_body='Invalid API call'
        return figure_response(msg_body, 400)
    else:
        if not bucket or not key:
            msg_body='Either of bucket or filepath not supplied !'
            return figure_response(msg_body, 400)
    
    #check presence of bucket and file
    response = s3_client.list_objects_v2(Bucket=bucket,Prefix=key)
    if response.get('KeyCount') == 0:
        msg_body='Key not found !'
        return figure_response(msg_body, 400)
    elif response.get('KeyCount') > 1:
        msg_body='Multiple keys returned'   #meaning the key is a directory
        return figure_response(msg_body, 400)
    else:
        #trigger glue ETL job 
        glue_status = trigger_glue_job('s3_rds_etl')
        msg_body=f'File {key} found in the bucket {bucket}. Job status: {glue_status}'
        return figure_response(msg_body, 200)
        
#function to trigger the glue ETL job
def trigger_glue_job(gluejobname):
    """
        Returns the status of the glue job supposed to be triggered
        :param gluejobname
        :return: status of gluejobname
    """
    try:
        runId = glue_client.start_job_run(JobName=gluejobname)
        status = glue_client.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as e:
        raise e
    else:
        return status['JobRun']['JobRunState']
        
#function to build  response object
def figure_response(msg_body, status_code):
    """
        Returns the dictionary Response object as per msg_body and status_code
        :param msg_body -> str
        :param status_code -> int
        :return: responseObject -> dict
    """
    responseObject['statusCode'] = status_code
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['body'] = json.dumps(msg_body)     
    return responseObject
