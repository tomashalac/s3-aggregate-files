# S3 Aggregate Files
Some Amazon services log events to S3 as small files, this program groups them in 128Mb files to make it easier to analyze them with Athena.


# Requirements
 * Serverless
 * Cost effective
 * Run on AWS lambda
 * Final file compatible with AWS Athena
 
 # Lambda Test Event
 It is recommended to put the maximum CPU and a high timeout in  AWS Lambda.
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
 
 # Use example
 You want to run athena on cloudfront logs, this script by grouping the files reduces the costs of S3.

 # Warning
 Use this code at **your own risk**, errors in amazon can become expensive, fast.
 
 # Services
 * AWS Lambda: https://aws.amazon.com/es/lambda/
 * AWS S3: https://aws.amazon.com/es/s3/
 * AWS Athena: https://aws.amazon.com/es/athena/
 * AWS CloudFront: https://aws.amazon.com/es/cloudfront/
 * AWS CloudFront Access Logs: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html