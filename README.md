# S3 Aggregate Files
Some Amazon services log events to S3 as small files, this program groups them in 128Mb files to make it easier to analyze them with Athena.
It also allows you to download an entire aws bucket, or a specific amount of files.


# Requirements
 * Serverless
 * Cost effective
 * Run on AWS lambda
 * Final file compatible with AWS Athena
 
 # Lambda Test Event
 It is recommended to put the maximum CPU and a high timeout in  AWS Lambda.
 * Handler: aggregateS3/main.lambda_handler
 * Runtime: Python3.7
 ```json
 {
  "bucket_download": "THE_NAME_OF_THE_BUCKET_FOR_DOWNLOAD",
  "bucket_upload": "THE_NAME_OF_THE_BUCKET_FOR_UPLOAD",
  "max_size_megas": 128,
  "min_size_megas": 100,
  "output_file": "%d/%m/%Y %H:%M:%S",
  "delete_old_file": false,
  "max_keys": 3000,
  "bucket_download_prefix": "folder/"
}
```

## Kinesis Firehose Config
 * Prefix: logs/!{timestamp:yyyy/MM/dd}/
 * Error prefix: errors/result=!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}/
 * Buffer size: 128
 * Buffer interval: 900
 * S3 compression: Disabled
 * S3 encryption: Disabled

## Required
 * bucket_download
 * bucket_upload
 
## Optional
 * max_size_megas **or** max_size_bytes
 * min_size_megas **or** min_size_bytes
 * output_file
 * delete_old_file
 * max_keys
 * bucket_download_prefix
 * only_download
 * local_folder_to_download
 * aws_access_key_id
 * aws_secret_access_key
 
 You can create a config.json file in case you are running it with terminal.
 
 # Use example
 You want to run athena on cloudfront logs, this script by grouping the files reduces the costs of S3.

 # Athena
 If empty records appear in the queries it is because of the cloudfront comments in the files,
  I recommend adding in the query 
  ```sql
  WHERE date is not null
  ```

 # Warning
 Use this code at **your own risk**, errors in amazon can become expensive, fast.
 
 # Services
 * AWS Lambda: https://aws.amazon.com/es/lambda/
 * AWS S3: https://aws.amazon.com/es/s3/
 * AWS Athena: https://aws.amazon.com/es/athena/
 * AWS CloudFront: https://aws.amazon.com/es/cloudfront/
 * AWS CloudFront Access Logs: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html
