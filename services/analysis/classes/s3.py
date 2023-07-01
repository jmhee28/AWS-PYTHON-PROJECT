import boto3
import dotenv
import os
import pandas as pd
dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
ACCESS_SECRET_KEY = os.environ["ACCESS_SECRET_KEY"]

class S3:
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, 
                    aws_secret_access_key=ACCESS_SECRET_KEY, 
                    region_name = 'ap-northeast-2')
    
    def get_object(self, Bucket, Key):
        return self.s3.get_object(Bucket, Key)
    
    def upload_fileobj(self, data, Bucket, Key):
        self.s3.upload_fileobj(data, Bucket, Key)

    def getCsvFile(self, Bucket, Key):
        try:
            response = self.s3.get_object(Bucket, Key)
            df = pd.read_csv(response['Body'], encoding='utf-8')
            return df
        except Exception as e:
            print('CSV 파일 읽기 실패:', e)