from aggregateS3 import aggregator, parallel_download, config
import os


EXTENSIONS_ALLOWED = ["txt", "log", "gz"]


def lambda_handler(event, context):
    config.CONFIG = config.Config(event)

    list_keys, suffix = parallel_download.download_all_files()

    return aggregator.aggregate_files(list_keys, suffix)