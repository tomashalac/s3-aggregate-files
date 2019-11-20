from multiprocessing import Process
import boto3
from aggregateS3 import main

def private_download(list_keys):
    s3 = boto3.client('s3')

    for file_key in list_keys:
        filename = "/tmp/" + file_key.replace('/', '_')
        s3.download_file(Bucket=main.BUCKET_DOWNLOAD, Key=file_key, Filename=filename)

def download_all_files():
    actual_count = 0
    s3 = boto3.client('s3')

    key_list, continuation_token = private_list_files_in_bukcet(s3, max_keys=main.MAX_KEYS)
    actual_count += len(key_list)
    private_start_parallel_download(key_list)

    while actual_count < main.MAX_KEYS and continuation_token:
        new_key_list, marker = private_list_files_in_bukcet(s3, continuation_token, main.MAX_KEYS - actual_count)
        actual_count += len(new_key_list)
        key_list = key_list + new_key_list
        private_start_parallel_download(key_list)

    return key_list


def private_list_files_in_bukcet(s3, continuation_token="", max_keys=1000):
    # AWS limit
    if max_keys > 1000:
        max_keys = 1000

    if continuation_token:
        response = s3.list_objects_v2(Bucket=main.BUCKET_DOWNLOAD, MaxKeys=max_keys, Prefix=main.BUCKET_DOWNLOAD_PREFIX, ContinuationToken=continuation_token)
    else:
        response = s3.list_objects_v2(Bucket=main.BUCKET_DOWNLOAD, MaxKeys=max_keys, Prefix=main.BUCKET_DOWNLOAD_PREFIX)

    print("Downloading "+str(len(response['Contents']))+" files from AWS S3")
    keys_list = []
    for file in response['Contents']:
        file_key = file["Key"]

        if file_key[-1] == "/":
            print("Skiping folder: " + file_key)
            continue

        keys_list.append(file_key)
    return keys_list, response["NextContinuationToken"]


def private_start_parallel_download(keys_list):
    # create a list to keep all processes
    processes = []

    total_files = len(keys_list)
    keys_list = list(private_chunks(keys_list, int(total_files/20)))

    for id in range(20):
        # create the process, pass instance and connection
        process = Process(target=private_download, args=(keys_list[id],))
        processes.append(process)

    # start all processes
    for process in processes:
        process.start()

    # make sure that all processes have finished
    for process in processes:
        process.join()

    print("All " + str(total_files) + " files downloaded.")
    return True


def private_chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
