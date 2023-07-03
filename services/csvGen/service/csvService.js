const AWS = require('aws-sdk');
const csv = require('csv-parser');
const fs = require('fs');

AWS.config.update({
  accessKeyId: ACCESS_KEY_ID,
  secretAccessKey: ACCESS_SECRET_KEY,
  region: 'ap-northeast-2'
});

const s3 = new AWS.S3();