	

import json
import sys
import asyncio
import os
import boto3

sys.path.append('/var/task/service')
# sys.path.append('/var/task/classes')
from analyzeService import *
from csvService import *
# from s3 import *

class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

def publish_sns_topic():
    sns = boto3.client('sns')
    topic_arn = os.environ['sliceCsvTopicArn']  # 환경 변수에서 SNS 토픽 ARN 값을 읽어옵니다.

    if topic_arn:
        params = {
            'Message': 'slice Csv',
            'TopicArn': topic_arn
        }
        response = sns.publish(**params)
        return response
    else:
        raise Exception('SNS 토픽 ARN이 환경 변수에 설정되어 있지 않습니다.')
                                        
def analysis(event, context):
    try:
        if 'resource' in event:
            resource = event['resource']
            if resource == "/anomal":
                result = getAnomally()
            elif resource == '/plot':
                makePlot()
                result = 'success'
            elif resource == '/graph':
                makeGraph()
                result = 'success'
            elif resource == '/group':
                data = getGroupInfo()
                result = json.dumps(data, cls=NumpyEncoder)
                response = {
                "statusCode": 200,
                "body": result
                }
                return response
            #slice
            elif resource == '/csv/dates':
                result = publish_sns_topic()
            response = {
                "statusCode": 200,
                "body": json.dumps(result)
            }
            return response
        elif 'Records' in event:
            message = event['Records'][0]['Sns']['Message']
            print("From SNS: " + message)
            loop = asyncio.get_event_loop()
            try:
                loop.run_until_complete(sliceCsv())
            except Exception as e:
                print('slice 오류 발생:', e)
            finally:
                loop.close()
            print("From SNS: " + message)
            return message
    except Exception as e:
        response = {
            'statusCode': 500,
            'body': 'An error occurred: ' + str(e)
        }
        return response
