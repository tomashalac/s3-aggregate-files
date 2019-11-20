from aggregateS3 import aggregator, parallel_download

BUCKET_DOWNLOAD = "THE-NAME-OF-YOU-S3-BUCKET-FOR-DOWNLOAD"
BUCKET_UPLOAD = "THE-NAME-OF-YOU-S3-BUCKET-FOR-UPLOAD"
MAX_SIZE_BYTES = 128 * 1024 * 1024 #Max 128MB
MIN_SIZE_BYTES = 100 * 1024 * 1024  #Min 100MB
OUTPUT_FILE = "%d/%m/%Y %H:%M:%S"
DELETE_OLD_FILE = False
MAX_KEYS = 1000
BUCKET_DOWNLOAD_PREFIX = ""

def lambda_handler(event, context):

    list_keys = parallel_download.download_all_files()

    return aggregator.aggregate_files(list_keys)