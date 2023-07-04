import sys
# sys.path.append('/var/task/classes')
# from s3 import S3
import asyncio
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

import boto3
import dotenv
import os
import pandas as pd
dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
ACCESS_SECRET_KEY = os.environ["ACCESS_SECRET_KEY"]
CSV_BUCKET_NAME  = 'edited-csvs'


s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, 
                  aws_secret_access_key=ACCESS_SECRET_KEY, 
                  region_name = 'ap-northeast-2')

def getCsvFile(bucket, filename):
    try:
        response = s3.get_object(Bucket=bucket, Key=filename)
        df = pd.read_csv(response['Body'], encoding='utf-8')
        return df
    except Exception as e:
        print('CSV 파일 읽기 실패:', e)

async def sliceByDate(year, month, date, df):
    df_date = df[df['기준일ID'] == date]
    csv_buffer = BytesIO()
    df_date.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    file_name = f'sliced/{year}{month}/{date}.csv'
    await s3.upload_fileobj(csv_buffer, CSV_BUCKET_NAME, file_name)   

async def sliceCsv(param):
    year, month  = param['year'], param['month']
    if len(month) == 1:
        month = '0'+ month   
    Key = f'reduced/LOCAL_PEOPLE_DONG_{year}{month}_reduced.csv'
    df = getCsvFile(CSV_BUCKET_NAME, Key)
    # # Get unique dates
    tasks = []
    dates = df['기준일ID'].unique()

    for date in dates:
        task = sliceByDate(year, month, date, df)
        tasks.append(task)

    await asyncio.gather(*tasks)