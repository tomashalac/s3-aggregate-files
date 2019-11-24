from aggregateS3 import aggregator, parallel_download, config
import boto3

EXTENSIONS_ALLOWED = ["txt", "log", "gz"]


def lambda_handler(event, context):
    print("Start of S3 Aggregate Files")
    config.CONFIG = config.Config(event)

    suffix, list_keys = parallel_download.download_all_files()

    if config.CONFIG.only_download:
        print("All "+str(len(list_keys))+" files were downloaded to the computer.")
        return True

    return aggregator.aggregate_files(list_keys, suffix)


def get_boto3(service="s3"):
    if config.CONFIG.aws_access_key_id:
        return boto3.client(
            service,
            aws_access_key_id=config.CONFIG.aws_access_key_id,
            aws_secret_access_key=config.CONFIG.aws_secret_access_key
        )

    return boto3.client(service)
