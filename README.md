# 서울시 공공 데이터 분석

## How to deploy

`install docker desktop`

`$ npm install -g serverless `

`$ cd services/analysis`

`$ sls deploy `

## 기능 설명

csv 데이터를 활용해 집 추정 위치 체류시간 과 배달 서비스 사용일수를 분석하여 데이터를 추출하고, matplotlib를 사용하여 그래프 이미지를 만들어 AWS S3에 저장
