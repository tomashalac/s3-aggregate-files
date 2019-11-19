# s3 aggregate files
Some Amazon services log events to S3 as small files, this program groups them in 128Mb files to make it easier to analyze them with Athena.


# Requirements
 * Serverless
 * Cost effective
 * Run on AWS lambda
 * Final file compatible with AWS Athena
 
 # Use example
You want to run athena on cloudfront logs, this script by grouping the files reduces the costs of S3.