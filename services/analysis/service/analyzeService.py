import json
import pandas as pd
from scipy.stats import zscore
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

import io
import numpy as np
import sys
sys.path.append('/var/task')
from classes.s3 import S3

s3 = S3()

IMG_BUCKET_NAME = "plt-images"
CSV_BUCKET_NAME  = 'edited-csvs'
CSV_FILE_NAME = '2023_3.csv'


def getAnomally():
    df = s3.getCsvFile(CSV_BUCKET_NAME, CSV_FILE_NAME)
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

def makePlot():
    df = s3.getCsvFile(CSV_BUCKET_NAME, CSV_FILE_NAME)
    df['총 집 추정 위치 체류시간'] = df['집 추정 위치 평일 총 체류시간'] + df['집 추정 위치 휴일 총 체류시간']

    df['총 배달 사용 일수'] = df['배달 서비스 사용일수'] + df['배달_브랜드 서비스 사용일수'] + df['배달_식재료 서비스 사용일수']

    grouped_df = df.groupby('행정동코드').agg({'행정동':'first', '총 집 추정 위치 체류시간':'sum', '총 배달 사용 일수':'sum'}).reset_index()

    plt.scatter(grouped_df['총 집 추정 위치 체류시간'], grouped_df['총 배달 사용 일수'])
    plt.xlabel('Total house estimated location stay time')
    plt.ylabel('Total Delivery Days')

    img_data = io.BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)
    s3.upload_fileobj(img_data, IMG_BUCKET_NAME, '2023_3_1.png')


    
def getGroupedDf():
    df = s3.getCsvFile(CSV_BUCKET_NAME, CSV_FILE_NAME)
    df['총 집 추정 위치 체류시간'] = df['집 추정 위치 평일 총 체류시간'] + df['집 추정 위치 휴일 총 체류시간']
    df['총 배달 사용 일수'] = df['배달 서비스 사용일수'] + df['배달_브랜드 서비스 사용일수'] + df['배달_식재료 서비스 사용일수']

    grouped_df = df.groupby('행정동코드').agg({'행정동':'first', '총 집 추정 위치 체류시간':'sum', '총 배달 사용 일수':'sum'}).reset_index()

    grouped_df['usage_home_time_ratio'] = grouped_df['총 배달 사용 일수'] / grouped_df['총 집 추정 위치 체류시간']

    small_threshold = grouped_df['usage_home_time_ratio'].quantile(0.25)
    large_threshold = grouped_df['usage_home_time_ratio'].quantile(0.75)
    def classify_ratio(ratio):
        if ratio < small_threshold:
            return 1  
        elif ratio > large_threshold:
            return 2  
        else:
            return 0 
        
    grouped_df['group'] = grouped_df['usage_home_time_ratio'].apply(classify_ratio)
    return grouped_df

def makeGraph():
    grouped_df = getGroupedDf()
    plt.figure(figsize=(10,6))
    plt.scatter(grouped_df['총 집 추정 위치 체류시간'], grouped_df['총 배달 사용 일수'], c=grouped_df['group'])
    plt.xlabel('Total house estimated location stay time')
    plt.ylabel('Total Delivery Days')
    plt.colorbar(ticks=[0,1,2])
    img_data = io.BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)
    s3.upload_fileobj(img_data, IMG_BUCKET_NAME, '2023_3_2.png')


def getGroupInfo():
    result = {}
    grouped_df = getGroupedDf()
    grouped_by_group = grouped_df.groupby(['group'])
    for name, group in grouped_by_group:
        unique_행정동 = group['행정동'].unique()
        result[name] = unique_행정동
    return result

        