import json
import pandas as pd

def handler(event, context):
    body = getAnomally()

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def getAnomally():
    df = pd.read_csv('2023_3.csv', encoding='utf-8')
    df['총 집 추정 위치 체류시간'] = df['집 추정 위치 평일 총 체류시간'] + df['집 추정 위치 휴일 총 체류시간']
    df['총 배달 사용 일수'] = df['배달 서비스 사용일수'] + df['배달_브랜드 서비스 사용일수'] + df['배달_식재료 서비스 사용일수']

    grouped_df = df.groupby('행정동코드').agg({'행정동':'first', '총 집 추정 위치 체류시간':'sum', '총 배달 사용 일수':'sum'}).reset_index()
    grouped_df['usage_home_time_ratio'] = grouped_df['총 배달 사용 일수'] / grouped_df['총 집 추정 위치 체류시간']
    grouped_df['usage_home_time_ratio_zscore'] = zscore(grouped_df['usage_home_time_ratio'])

    threshold = 3

    anomalies = grouped_df[(grouped_df['usage_home_time_ratio_zscore'].abs() > threshold)]
    result = {}
    for index, row in anomalies.iterrows():
        result[row['행정동']] = { 
           "usage_home_time_ratio":row['usage_home_time_ratio'], 
           "Z-score":row['usage_home_time_ratio_zscore']  
           }
        
    return result